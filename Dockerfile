FROM icr.io/codeengine/golang:alpine
COPY dispatcher.go /
COPY go.mod /
COPY go.sum /
RUN  go get github.com/IBM/cloudant-go-sdk/cloudantv1@v0.7.6
RUN  go build -o /dispatcher /dispatcher.go

# Copy the exe into a smaller base image
FROM icr.io/codeengine/alpine
COPY --from=0 /dispatcher /dispatcher
CMD  /dispatcher