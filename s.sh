#!/bin/bash

SLURM_VER="slurm-14.03.0"
BRANCH="ostrich_branch"

OWNER=$(whoami)
HOSTNAME=$(hostname)

APP_DIR="deployment"
LINKS_DIR="links"

PROJECT_ROOT=$(pwd)
APP_ROOT="${PROJECT_ROOT}/${APP_DIR}"
LINKS_ROOT="${PROJECT_ROOT}/${LINKS_DIR}"

#echo $HOSTNAME
#echo $PROJECT_ROOT
#echo $APP_ROOT
#echo $LINKS_ROOT

if [ "$1" = "startup" ] || [ "$1" = "reset" ]; then

    if [ "$1" = "startup" ] && [ -e "$SLURM_VER" ]; then
        echo "Directory already exists:" $SLURM_VER
        echo "Use 'reset' to force the operation"
        exit 1
    fi

    rm -rf $APP_DIR
    rm -rf $LINKS_DIR

    mkdir -p ${APP_DIR}
    mkdir ${APP_DIR}"/var" ${APP_DIR}"/etc"

    rm -rf $SLURM_VER
    tar -xf ${SLURM_VER}".tar.bz2"

    cp -r ${BRANCH}/* ${SLURM_VER}/

    cd $SLURM_VER
    ./autogen.sh || exit 1
    ./configure --prefix=$APP_ROOT --enable-multiple-slurmd --without-munge --with-ssl
    make -s
    make -s install
    cd ..

elif [ "$1" = "links" ]; then

    mkdir -p ${LINKS_DIR}
    ln -sf ${APP_ROOT}/bin/* -t ${LINKS_ROOT}
    ln -sf ${APP_ROOT}/sbin/* -t ${LINKS_ROOT}
    ln -sf ${APP_ROOT}/var -t ${LINKS_ROOT}
    ln -sf ${APP_ROOT}/etc -t ${LINKS_ROOT}

elif [ "$1" = "config" ] || [ "$1" = "db_config" ]; then

    if [ "$1" = "config" ]; then
        DEST_FILE="/etc/slurm.conf"
    else
        DEST_FILE="/etc/slurmdbd.conf"
    fi

    if [ $# -lt 2 ]; then
        echo "missing config filename"
    else
        cat $2 | sed s@"{HOSTNAME}"@$HOSTNAME@g | sed s@"{OWNER}"@$OWNER@g          \
        | sed s@"{PROJECT_ROOT}"@$PROJECT_ROOT@g | sed s@"{APP_ROOT}"@$APP_ROOT@g   \
        > ${APP_DIR}${DEST_FILE}
    fi

elif [ "$1" = "make" ]; then

    SCHED_DIR="/src/plugins/priority/ostrich"

    cp -r ${BRANCH}${SCHED_DIR}/* ${SLURM_VER}${SCHED_DIR}/

    SCHEDULERS=${SLURM_VER}${SCHED_DIR}

    make -s -C $SCHEDULERS

    if [ $? -eq 0 ]; then
        make -s -C $SCHEDULERS install &> /dev/null
        echo "done"
    else
        echo "errors found"
    fi

elif [ "$1" = "cleanup" ]; then

    rm -rf $SLURM_VER $APP_DIR $LINKS_DIR
    python clean.py

elif [ "$1" = "patch" ]; then

    BEFORE="TMP_BEFORE"
    AFTER="TMP_AFTER"

    mkdir $BEFORE $AFTER
    tar -xf ${SLURM_VER}".tar.bz2" -C $BEFORE --strip-components=1
    tar -xf ${SLURM_VER}".tar.bz2" -C $AFTER --strip-components=1

    python clean.py
    cp -r ${BRANCH}/* ${AFTER}/
    diff -Naur $BEFORE $AFTER > "ostrich_patch_"${SLURM_VER}

    rm -rf $BEFORE $AFTER

else
    echo "Available commands:"
    echo "startup/reset, links, config, make, cleanup, patch"
fi
