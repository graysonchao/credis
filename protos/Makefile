all:
	protoc -I. --grpc_out=./src --cpp_out=./src --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` *.proto
clean:
	rm src/*
