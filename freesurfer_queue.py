

import os
import json
import shutil
import subprocess
import time
from pathlib import Path

QUEUE_ROOT = Path("fs_queue")
MAX_RUNTIME = 20 * 3600  # 20 hours
POLL_INTERVAL = 30
MAX_CONCURRENT = 10


def load_config(job_dir):
    with open(job_dir / "config.json") as f:
        return json.load(f)


def build_command(config, job_dir):
    subject_id = config["subject_id"]
    subjects_dir = Path(config["subjects_dir"])
    input_dir = job_dir / "input"
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory {input_dir} does not exist")

    if not subjects_dir.exists():
        raise FileNotFoundError(f"Subjects directory {subjects_dir} does not exist")

    env_script = config["env_script"]
    if not Path(env_script).exists():
        raise FileNotFoundError(f"Environment script {env_script} does not exist")

    # use bash -c to source environment
    cmd = f"""
    source "{env_script}" && \
    export SUBJECTS_DIR="{subjects_dir}" && \
    recon-all -i "{input_dir}" -s "{subject_id}" -all
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

        if not alive:
            print(f"[DONE] {job_dir.name}")
            shutil.move(str(job_dir), QUEUE_ROOT / "done" / job_dir.name)
            continue

        # Timeout
        if elapsed > MAX_RUNTIME:
            print(f"[TIMEOUT] Killing {job_dir.name}")

            try:
                os.killpg(os.getpgid(pid), 9)
            except Exception as e:
                print(f"Kill failed: {e}")

            shutil.move(str(job_dir), QUEUE_ROOT / "timeout" / job_dir.name)


def start_jobs_if_possible():
    queued_dir = QUEUE_ROOT / "queued"
    running_dir = QUEUE_ROOT / "running"

    running_jobs = get_running_jobs()
    available_slots = MAX_CONCURRENT - len(running_jobs)

    if available_slots <= 0:
        return

    jobs = sorted([d for d in queued_dir.iterdir() if d.is_dir()])

    for job in jobs[:available_slots]:
        target = running_dir / job.name
        shutil.move(str(job), target)

        print(f"[START] {job.name}")
        start_job(target)


def ensure_dirs():
    for d in ["queued", "running", "done", "failed", "timeout"]:
        (QUEUE_ROOT / d).mkdir(parents=True, exist_ok=True)


def main():
    ensure_dirs()

    while True:
        try:
            check_running_jobs()
            start_jobs_if_possible()
        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()






