FROM icr.io/codeengine/golang:alpine
COPY dispatcher.go /
RUN  go build -o /dispatcher /dispatcher.go

# Copy the exe into a smaller base image
FROM icr.io/codeengine/alpine
COPY --from=0 /dispatcher /dispatcher
CMD  /dispatcher