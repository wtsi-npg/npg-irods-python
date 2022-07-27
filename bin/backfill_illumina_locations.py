import subprocess

CREATION_SCRIPT = "./create_illumina_irods_location_file.py"
BACKFILL_DIR = "./backfill_json"
PROCESSES = 8
LOGS_DIR = "./backfill_logs"
PERL = "PERL5LIB=$PERL5LIB:/software/npg/current/lib/perl5"
LOADING_SCRIPT = "/software/bin/npg_irods_locations2ml_warehouse"
IN_PROGRESS_DIR = "./in_progress"

# for current in range(45):
current = 30  # while testing
colls = [
    f"/seq/illumina/runs/{str(current)}/{str(current)}{str(i).zfill(3)}"
    for i in range(1000)
] + [f"/seq/{(str(current) + str(i).zfill(3)).lstrip('0')}" for i in range(1000)]

create = subprocess.run(
    [
        CREATION_SCRIPT,
        "-v",
        "-p",
        PROCESSES,
        "-o",
        f"{BACKFILL_DIR}/{current}xxx.json",
    ]
    + colls,  # Append items from collection list to command list
    capture_output=True,
    encoding="utf8",
)

with open(f"{LOGS_DIR}/{current}xxx_creation.log", "w") as log:
    log.write(create.stderr)

subprocess.run(
    ["mv", f"{LOGS_DIR}/{current}xxx.json", f"{IN_PROGRESS_DIR}/{current}xxx.json"]
)

load = subprocess.run(
    [
        PERL,
        LOADING_SCRIPT,
        "--path",
        f"{IN_PROGRESS_DIR}/{current}xxx.json",
        "--verbose",
        "--dry-run",  # while testing
    ],
    capture_output=True,
    encoding="utf8",
)

with open(f"{LOGS_DIR}/{current}xxx_loading.log", "w") as log:
    log.write(load.stdout)
    log.write(load.stderr)
