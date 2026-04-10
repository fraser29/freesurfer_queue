import os
import shutil


def detect_zombie_process(pid):
    try:
        with open(f'/proc/{pid}/stat', 'r') as f:
            # Reading the stat file
            stat_info = f.read().split()
            # The state is the third field in the stat file
            state = stat_info[2]
            return state == 'Z'
    except IOError:
        # Process has already finished
        return False


def handle_process(pid):
    if detect_zombie_process(pid):
        # Move the process to the failed directory if it's a zombie
        shutil.move(f'queue/{pid}', 'failed/')
    else:
        # Process executed successfully
        shutil.move(f'queue/{pid}', 'done/')
