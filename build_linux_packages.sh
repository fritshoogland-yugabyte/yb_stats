#docker build --rm --progress plain --output linux_packages --file Dockerfile.build_centos_7 .
docker build --rm --output . --file Dockerfile.build_centos_7 .
docker build --rm --output . --file Dockerfile.build_alma_8 .
docker build --rm --output . --file Dockerfile.build_alma_9 .
docker build --rm --progress plain --output . --file Dockerfile.build_ubuntu_2204 .
