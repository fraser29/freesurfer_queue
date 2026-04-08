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
python freesurfer_queue.py
```

The runner continuously polls and schedules queued jobs.

## Notes

- Concurrency, poll interval, and timeout are set at the top of `freesurfer_queue.py`.
- FreeSurfer is initialized per job via `source "$env_script"` before running `recon-all`.
