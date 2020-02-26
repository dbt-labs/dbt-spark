#!/bin/bash

# Echo this entire script to a logfile.txt
exec > >(tee -a $HOME/logfile.txt) 2>&1

DEFAULT_CMD="bash"

detect_hostname()
{
    echo "Initializing hostname (needed for AWS ECS)..."
    echo "Detecting 'eth1' interface..."
    DETECTED_IP=$(ifconfig -a | grep -A2 eth1 | grep inet | awk '{print $2}' | sed 's#/.*##g' | grep "\.")
    if [[ -z $DETECTED_IP ]]; then
        echo "Detecting 'eth0' interface ('eth1' not found)..."
        DETECTED_IP=$(ifconfig -a | grep -A2 eth0 | grep inet | awk '{print $2}' | sed 's#/.*##g' | grep "\." | head -1)
    fi
    DETECTED_HOSTNAME=$(hostname)
    echo -e "\n\nDETECTED_IP=$DETECTED_IP\nDETECTED_HOSTNAME=$DETECTED_HOSTNAME\n\n"
    # NOTE: newer OS versions use `ip` instead of `ifconfig`
    echo -e "Current file contents:\n $(cat /etc/hosts)"
    echo "$DETECTED_IP $DETECTED_HOSTNAME" >> /etc/hosts
    echo -e "\n\n\nUpdated file contents:\n $(cat /etc/hosts)"
}

clone_git_repo() {
    echo "Cloning project from git ($PROJECT_GIT_URL)..."
    git clone $PROJECT_GIT_URL /projects/project-git
    cd /projects/project-git
    if [[ ! -z "$PROJECT_COMMIT" ]]; then
        echo "Checking out the project commit: $PROJECT_COMMIT"
        git checkout $PROJECT_COMMIT
    elif [[ ! -z "$PROJECT_BRANCH" ]]; then
        echo "Checking out the project commit: $PROJECT_BRANCH"
        git checkout $PROJECT_BRANCH
    fi
}

start_mysql()
{
    echo "Starting MySQL server as metastore (service mysql start)..."
    service mysql start
    if [[ ! -f "/var/lib/mysql/mysql-init.sql" ]]; then
        echo "Creating MySQL config script (/var/lib/mysql/mysql-init.sql)..."
        echo "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('root');\n" > /var/lib/mysql/mysql-init.sql
        echo "USE mysql;" >> /var/lib/mysql/mysql-init.sql
        echo "UPDATE user SET authentication_string=PASSWORD('root') WHERE user='root';" >> /var/lib/mysql/mysql-init.sql
        echo "UPDATE user SET plugin='mysql_native_password' WHERE user='root';" >> /var/lib/mysql/mysql-init.sql
        echo "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('root');\n" >> /var/lib/mysql/mysql-init.sql
        echo "FLUSH privileges;" >> /var/lib/mysql/mysql-init.sql
        # echo "MySQL Config File:"
        # cat /var/lib/mysql/mysql-init.sql
        echo "Running MySQL init script (/var/lib/mysql/mysql-init.sql)..."
        cat /var/lib/mysql/mysql-init.sql | mysql -uroot mysql
    fi
    echo "MySQL process ID is $(cat /var/run/mysqld/mysqld.pid)"
}

start_spark()
{
    mkdir -p $SPARK_WAREHOUSE_DIR
    python3 -m pip install --upgrade slalom.dataops
    python3 -m slalom.dataops.sparkutils start_server --daemon
}

if [[ ! -z "$DETECT_HOSTNAME" ]]; then
    # Set env var in order to initialize hostname
    detect_hostname;
fi

if [[ ! -z "$PROJECT_GIT_URL" ]]; then
    clone_git_repo;
fi

set -e  # Fail script on error
CMD="$@"  # Set command to whatever args were sent to the bootstrap script
if [[ -z "$CMD" ]]; then
    echo "No command provided in bootstrap script. Using default command: $DEFAULT_CMD"
    CMD=$DEFAULT_CMD
elif [[ "$1" == "dbt-spark" || ! -z "$WITH_SPARK" ]]; then
    echo "Initializing spark for command: $CMD";
    start_mysql;
    start_spark;
    echo "Running CMD from bootstrap args: $CMD"
else
    if [[ "$1" == "s-spark" && "$METASTORE_TYPE" == "MySQL" ]]; then
        echo "Command is 's-spark' and METASTORE_TYPE='MySQL'. Starting MySQL Server..."
        start_mysql;
    fi
    echo "Running CMD from bootstrap args: $CMD"
fi

set +e  # Ignore errors (so we can clean up afterwards)
$CMD
RETURN_CODE=$?  # Capture the return code so we can print it
set -e  # Re-enable failure on error
echo -e "Bootstrap script completed.\nRETURN_CODE=$RETURN_CODE\nCMD=$CMD"

exit $RETURN_CODE  # Return the error code (zero if successful)
