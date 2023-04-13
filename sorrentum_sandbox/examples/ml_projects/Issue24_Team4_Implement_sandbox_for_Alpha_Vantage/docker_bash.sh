docker build . -f Dockerfile -t alpha_vantage
docker run -p 8888:8888 alpha_vantage
