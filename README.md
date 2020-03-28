# abackup

A Backup System

A collection of tools for backups (for docker), monitoring storage health, and syncing data between hosts.

Notifications are provided through Slack.

Built all in Python 3.

## Requirements
Required Python 3 packages:
* click
* mdstat
* python-cronTab
* tabulate

## Base configuration
The base configuration provides settings for all the tools. Here you can specify the log root directory and notification
settings. The tools expect to find these settings in a file named `conf.yml` in the same directory as the specific 
config files for the individual tools (this defaults to `~/.abackup`). 

```
log_root: '~/.abackup/logs/foo' # optional
notification: # optional
  slack: # optional
    api_url: 'https://hooks.slack.com/services/foo/bar'
    username: 'abackup' #optional
    channel: '#server-alerts' #optional
```

## abackup

A tool for backups and restorations (primarily for docker containers).

Currently it uses tar to create archieves of directories in a container and mysqldump to create mysql backups. The
backups are copied inside the backup_root (see configuration below). The directory structure is based on the project
name and container name: `$backup_root/$project_name/$container_name/$db_name.sql`, `$backup_root/$project_name/$container_name/$dir_name.tar.gz`, etc.

### Configuration
The specific config file for the abackup tool has the following settings:

```
backup_root: /path/to/directory/to/store/backups
permissions: # optional
  group: abackup # optional
  directories: "2770" # optional
  files: "660" # optional
```

#### Project Config
The abackup tool also requires a project level config file for each project you want to backup. These files are typically
managed along with your project files (e.g. kept at the root of your git repo with your docker-compose file) and named `.abackup.yml`.

```
containers:
  test-container:
    databases: # optional
      - name: testdb1
        driver: mysql
        password: "foobar"
      - name: testdb2
        driver: msyql
        password: "barfoo"

    directories: # optional
      - /data/foo
      - /data/bar

    backup: # optional
      pre_commands: # optional
        - command_type: docker
          command_string: mkdir -p /data/foo/bar
          in_container: True # optional
          docker_options: [] # optional
        - echo "foobar"
      post_commands: # optional
        - command_type: docker
          command_string: echo "even more foo" >> /data/foo/t1.txt
          in_container: True # optional
          docker_options: [] # optional
      version_count: 1 # optional
      auto_backup: # optional
        - frequency: "0 0 * * *"
          notify: "always"
      docker_options: [] # optional

    restore:
      pre_commands: # optional
        - command_type: docker
          command_string: mkdir -p /data/foo/bar
          in_container: True # optional
          docker_options: [] # optional
        - echo "foobar"
      post_commands: # optional
        - command_type: docker
          command_string: echo "even more foo" >> /data/foo/t1.txt
          in_container: True # optional
          docker_options: [] # optional
      version_count: 1 # optional
      auto_backup: # optional
        - frequency: "0 0 * * *"
          notify: "always"
      docker_options: [] # optional
```

## abdata

A tool for monitoring storage health. Works for ZFS pools and mdadm managed raid disks.

### Configuration
The specific config file for the abdata tool has the following settings:

```
drivers:
  mdadm:
    pools:
      - name: md0
        path: /mnt/md0
        auto_check: # optional
          - frequency: "0 2 * * *" # optional
            notify: auto # optional
          - frequency: "0 3 * 1 *" # optional
            notify: always # optional
  zfs:
    pools:
      - name: mainpool
        path: /mainpool
        auto_check: # optional
          - frequency: "0 2 * * *" # optional
            notify: auto # optional
          - frequency: "0 3 * 1 *" # optional
            notify: always # optional
```

## absync

A tool for syncing data between hosts. Works over ssh with rsync.

### Configuration
The specific config file for the absync tool has the following settings:

```
owned_data: # optional
  o1:
    path: /path/to/owned/data/o1
    options: # optional
      delete: false # optional
      max_delete: 100 # optional
    auto_sync: # optional
      - remote_name: r1
        notify: "never" # optional
        frequency: "0 1 * * *" # optional
        options: # optional
          delete: true # optional
      - remote_name: r2

  o2:
    path: /path/to/owned/data/o2

stored_data:  # optional
  s1:
    path: /path/to/stored/data/s1
  s2:
    path: /path/to/stored/data/s2

remotes:  # optional
  r1:
    host: foo.bar.bz
    port: 2222 # optional
    user: foo # optional 
    ssh_key: /home/foo/.ssh/id_rsa # optional
  r2:
    host: some.domain.xyz
```
