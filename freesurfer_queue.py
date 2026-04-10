

import os
import json
import shutil
import subprocess
import sys
import time
import logging
from pathlib import Path


# Default runtime settings - can be overridden by .env file
QUEUE_ROOT = Path("fs_queue")
MAX_RUNTIME = 20 * 3600  # seconds
POLL_INTERVAL = 30  # seconds
MAX_CONCURRENT = 10
QUEUE_ROOT_MUST_BE_MOUNT = False
LOGGER = logging.getLogger("freesurfer_queue")
LOGLEVEL = "INFO"

def load_config(job_dir):
    with open(job_dir / "config.json") as f:
        return json.load(f)


def build_command(config, job_dir):
    subject_id = config["subject_id"]
    input_dir = job_dir / "input"
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")
    file_list = os.listdir(input_dir)
    if len(file_list) == 0:
        raise FileNotFoundError(f"Input directory {input_dir} is empty")
    input_file = os.path.join(input_dir, file_list[0])

    env_script = config["env_script"]
    if not Path(env_script).exists():
        raise FileNotFoundError(f"Environment script {env_script} does not exist")

    # use bash -c to source environment
    cmd = f"""
    source "{env_script}" && \
    recon-all -i "{input_file}" -s "{subject_id}" -all
    """

    return ["bash", "-c", cmd]


def start_job(job_dir):
    config = load_config(job_dir)

    logfile = job_dir / "recon.log"

    cmd = build_command(config, job_dir)

    log = open(logfile, "ab")

    proc = subprocess.Popen(
        cmd,
        stdout=log,
        stderr=log,
        stdin=subprocess.DEVNULL,
        preexec_fn=os.setsid  # isolate process group
    )

    meta = {
        "pid": proc.pid,
        "start_time": time.time()
    }

    with open(job_dir / "runtime.json", "w") as f:
        json.dump(meta, f)

    return proc.pid


def get_running_jobs():
    running_dir = QUEUE_ROOT / "running"
    return [d for d in running_dir.iterdir() if d.is_dir()]


def check_running_jobs():
    running_jobs = get_running_jobs()

    for job_dir in running_jobs:
        runtime_file = job_dir / "runtime.json"
        if not runtime_file.exists():
            LOGGER.debug("No runtime.json for %s", job_dir.name)
            continue

        with open(runtime_file) as f:
            meta = json.load(f)

        pid = meta["pid"]
        start_time = meta["start_time"]
        elapsed = time.time() - start_time

        # Check alive
        try:
            os.kill(pid, 0)
            alive = True
        except OSError:
            alive = False

        LOGGER.debug(f"Checking {job_dir.name} is alive: {alive}")
        
        if not alive:
            LOGGER.info(f"[DONE] {job_dir.name} - NOTE - no check on success of failure ")
            shutil.move(str(job_dir), QUEUE_ROOT / "done" / job_dir.name)
            LOGGER.info(f"{job_dir.name} moved to done ")
            continue

        # Timeout
        if elapsed > MAX_RUNTIME:
            LOGGER.warning("[TIMEOUT] Killing %s", job_dir.name)

            try:
                os.killpg(os.getpgid(pid), 9)
            except Exception as e:
                LOGGER.exception("Kill failed for %s: %s", job_dir.name, e)

            shutil.move(str(job_dir), QUEUE_ROOT / "timeout" / job_dir.name)
            LOGGER.info(f"{job_dir.name} moved to timeout ")

# TODO
"""
no check if complete or failed - if not alive - check is complete - standard line in fs: 

Started at Thu 9 Apr 17:40:43 CEST 2026
Ended   at Fri 10 Apr 01:26:05 CEST 2026
#@#%# recon-all-run-time-hours 7.756
recon-all -s subj_test_fraser finished without error at Fri 10 Apr 01:26:05 CEST 2026
done

then move to done, else in failed. 


"""


def start_jobs_if_possible():
    queued_dir = QUEUE_ROOT / "queued"
    running_dir = QUEUE_ROOT / "running"

    running_jobs = get_running_jobs()
    available_slots = MAX_CONCURRENT - len(running_jobs)

    if available_slots <= 0:
        return

    jobs = sorted([d for d in queued_dir.iterdir() if d.is_dir()])
    # Only if config file exists
    jobs = [i for i in jobs if "config.json" in os.listdir(i)]

    for job in jobs[:available_slots]:
        target = running_dir / job.name
        shutil.move(str(job), target)

        LOGGER.info("[START] %s", job.name)
        start_job(target)


def ensure_dirs():
    for d in ["queued", "running", "done", "failed", "timeout"]:
        (QUEUE_ROOT / d).mkdir(parents=True, exist_ok=True)


def _parse_env_file(env_path):
    values = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _queue_root_error():
    if not QUEUE_ROOT.exists():
        return f"Queue root directory {QUEUE_ROOT} does not exist"
    if not QUEUE_ROOT.is_dir():
        return f"Queue root path {QUEUE_ROOT} is not a directory"
    if not os.access(QUEUE_ROOT, os.W_OK | os.X_OK):
        return f"Queue root directory {QUEUE_ROOT} is not writable"
    if QUEUE_ROOT_MUST_BE_MOUNT and not QUEUE_ROOT.is_mount():
        return (
            f"Queue root {QUEUE_ROOT} is not a mounted filesystem "
            "(QUEUE_ROOT_MUST_BE_MOUNT=true)"
        )
    return None


def ensure_queue_root_available(exit_on_error=True):
    error = _queue_root_error()
    if not error:
        return True

    message = f"[CONFIG ERROR] {error}"
    if LOGGER.handlers:
        LOGGER.error(message)
    else:
        print(message)
    if exit_on_error:
        sys.exit(1)
    return False


def load_runtime_settings():
    env_path = Path(".env")
    if not env_path.exists():
        print("[CONFIG ERROR] Missing .env file in project root.")
        print("Create it from .env.example and adjust values:")
        print("  cp .env.example .env")
        print("  # then edit .env")
        sys.exit(1)

    values = _parse_env_file(env_path)
    required = ["QUEUE_ROOT", "MAX_RUNTIME", "POLL_INTERVAL", "MAX_CONCURRENT"]
    missing = [k for k in required if k not in values or not values[k]]
    if missing:
        print(f"[CONFIG ERROR] Missing required .env keys: {', '.join(missing)}")
        print("See .env.example for required settings.")
        sys.exit(1)

    global QUEUE_ROOT, MAX_RUNTIME, POLL_INTERVAL, MAX_CONCURRENT, QUEUE_ROOT_MUST_BE_MOUNT, LOGLEVEL
    try:
        QUEUE_ROOT = Path(values["QUEUE_ROOT"])
        MAX_RUNTIME = int(values["MAX_RUNTIME"])
        POLL_INTERVAL = int(values["POLL_INTERVAL"])
        MAX_CONCURRENT = int(values["MAX_CONCURRENT"])
        QUEUE_ROOT_MUST_BE_MOUNT = _parse_bool(values.get("QUEUE_ROOT_MUST_BE_MOUNT"), False)
        LOGLEVEL = values.get("LOGLEVEL", "INFO")
    except ValueError as exc:
        print(f"[CONFIG ERROR] Invalid numeric value in .env: {exc}")
        print("Expected integers for MAX_RUNTIME, POLL_INTERVAL, and MAX_CONCURRENT.")
        sys.exit(1)

    ensure_queue_root_available(exit_on_error=True)


def configure_logging():
    logfile = QUEUE_ROOT / "freesurfer_queue.log"
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    LOGGER.setLevel(LOGLEVEL)
    LOGGER.handlers.clear()

    file_handler = logging.FileHandler(logfile, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    LOGGER.addHandler(file_handler)
    LOGGER.addHandler(stream_handler)
    LOGGER.propagate = False
    LOGGER.info("Logging initialized. Writing to %s", logfile)
    LOGGER.debug("Logging level is DEBUG - expect a high output")


def main():
    load_runtime_settings()
    ensure_dirs()
    configure_logging()
    ensure_queue_root_available(exit_on_error=True)
    LOGGER.info(
        "Queue started: root=%s max_concurrent=%s poll_interval=%ss max_runtime=%ss",
        QUEUE_ROOT,
        MAX_CONCURRENT,
        POLL_INTERVAL,
        MAX_RUNTIME,
    )

    while True:
        if os.path.isfile(os.path.join(QUEUE_ROOT, "kill_fs_queue")):
            LOGGER.info("FOUND KILL FILE - EXITING" ) 
            break
        try:
            ensure_queue_root_available(exit_on_error=True)
            check_running_jobs()
            start_jobs_if_possible()
        except Exception as e:
            LOGGER.exception("[ERROR] Queue loop failure: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()






