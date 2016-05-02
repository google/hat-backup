#!/bin/sh
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
