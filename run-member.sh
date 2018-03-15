verbose='false'
port='6379'
hostname='127.0.0.1'
gcs_mode='0'
module_path='./build/src'

usage() {
    echo "Usage: run-member [-g gcs_mode] [-m module_path] [-p port] [-H hostname]"
    echo "-g: GCS mode to run with, default 0"
    echo "-m: location of libmember.so, default ./build/src"
    echo "-p: port to listen on, default 6379"
    echo "-H: hostname to listen on, default 0.0.0.0"
    echo "-h: print this help and exit"
}

while getopts 'g:m:p:r:H:h' flag; do
  case "${flag}" in
    g) gcs_mode="${OPTARG}" ;;
    m) module_path="${OPTARG}" ;;
    p) port="${OPTARG}" ;;
    H) hostname="${OPTARG}" ;;
    h) usage; exit ;;
    *) usage; exit ;;
  esac
done

redis-server --protected-mode no --loadmodule $module_path/libmember.so $hostname $port --port $port
