#!/bin/sh
# The purpose of this file is to install Cap'n Proto in
# the Travis CI environment. Outside this environment,
# you would probably not want to install it like this.

set -e

# check if libsodium is already installed
if [ ! -d "$HOME/capnp/lib" ]; then
  wget https://capnproto.org/capnproto-c++-0.5.3.tar.gz
  tar zxf capnproto-c++-0.5.3.tar.gz
  cd capnproto-c++-0.5.3
  ./configure --prefix=$HOME/capnp
  make
  make install
else
  echo 'Using cached directory.'
fi
