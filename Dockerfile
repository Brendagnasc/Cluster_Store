# Dockerfile dos nos C++ (store_node, sync_node, client).
# A mesma imagem serve para os tres: o docker-compose escolhe o binario no command.
FROM gcc:13
WORKDIR /app
COPY Makefile ./
COPY include include
COPY src src
RUN make
ENV DOCKER_MODE=1
CMD ["./bin/client", "0"]
