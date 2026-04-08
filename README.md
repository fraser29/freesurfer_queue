# FreeSurfer Queue

A lightweight file-system queue runner for freesurfer `recon-all` jobs.

## What it does

- Watches `fs_queue/queued` for jobs.
- Starts up to `MAX_CONCURRENT` jobs in parallel.
- Moves finished jobs to `fs_queue/done`.
- Kills jobs that exceed `MAX_RUNTIME` and moves them to `fs_queue/timeout`.
- Writes run logs to each job's `recon.log`.

## Queue layout

```
fs_queue/
  queued/
  running/
  done/
  timeout/
  failed/
```

Each job is a directory (e.g. `fs_queue/queued/job_001`) containing:

- `config.json`
- `input` (input image path used by `recon-all -i`)

## `config.json` (required fields)

```json
{
  "subject_id": "patient_001",
  "subjects_dir": "/data/freesurfer_subjects",
  "env_script": "/opt/freesurfer/7.3/SetUpFreeSurfer.sh"
}
```

See `example_config.json` for a full example.

## Run

```bash
cp .env.example .env
```

Edit `.env` as needed, then start the runner:

```bash
python freesurfer_queue.py
```

The runner continuously polls and schedules queued jobs.

## Run as Ubuntu service (systemd)

1) Create and edit local runtime config:

```bash
cd /path/to/freesurfer_queue
cp .env.example .env
```

2) Create `/etc/systemd/system/freesurfer-queue.service`:

```ini
[Unit]
Description=FreeSurfer Queue Runner
After=network.target
RequiresMountsFor=/path/to/queue_root_mount

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/path/to/freesurfer_queue
ExecStart=/usr/bin/python3 /path/to/freesurfer_queue/freesurfer_queue.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

3) Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now freesurfer-queue
```

4) Check status/logs:

```bash
systemctl status freesurfer-queue
journalctl -u freesurfer-queue -f
```

Replace `User` and paths with your actual deployment values.
If `QUEUE_ROOT` is on network storage, set `RequiresMountsFor` to the mount path.

## Notes

- Runtime settings are loaded from `.env` (`QUEUE_ROOT`, `MAX_RUNTIME`, `POLL_INTERVAL`, `MAX_CONCURRENT`).
- Set `QUEUE_ROOT_MUST_BE_MOUNT=true` when `QUEUE_ROOT` is a mount point that must be present.
- Queue service logs are written to `QUEUE_ROOT/queue.log` (and also emitted to console).
- FreeSurfer is initialized per job via `source "$env_script"` before running `recon-all`.
