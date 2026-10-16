// Echo backend for tests/bench, flags pick how each port behaves. Every method echoes the request
// bytes back, which decode as the same message type, so no generated code is needed.
//
//	go run . -ok 16 -latency 500us
//
// 50051 to 50051+ok-1 answer after -latency, 50100 answers after -slow, 50101 never answers and 50102 restarts
// every -flap. Nothing listens on 50199, which stands in for a dead host.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return *(v.(*[]byte)), nil }

func (rawCodec) Unmarshal(data []byte, v any) error {
	*(v.(*[]byte)) = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

func echo(delay time.Duration, hang bool) grpc.StreamHandler {
	return func(_ any, stream grpc.ServerStream) error {
		var req []byte
		if err := stream.RecvMsg(&req); err != nil {
			return err
		}
		if hang {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		return stream.SendMsg(&req)
	}
}

func serve(ctx context.Context, port int, handler grpc.StreamHandler) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(handler))
	go func() {
		<-ctx.Done()
		srv.Stop()
	}()
	return srv.Serve(lis)
}

func main() {
	ok := flag.Int("ok", 16, "number of normal ports starting at 50051")
	latency := flag.Duration("latency", 500*time.Microsecond, "delay of the normal ports")
	slow := flag.Duration("slow", 200*time.Millisecond, "delay of port 50100")
	flap := flag.Duration("flap", 500*time.Millisecond, "how long port 50102 stays up and down")
	flag.Parse()
	encoding.RegisterCodec(rawCodec{})

	ctx := context.Background()
	for i := 0; i < *ok; i++ {
		go func(port int) { log.Fatal(serve(ctx, port, echo(*latency, false))) }(50051 + i)
	}
	go func() { log.Fatal(serve(ctx, 50100, echo(*slow, false))) }()
	go func() { log.Fatal(serve(ctx, 50101, echo(0, true))) }()
	go func() {
		for {
			up, stop := context.WithTimeout(ctx, *flap)
			_ = serve(up, 50102, echo(*latency, false))
			stop()
			time.Sleep(*flap)
		}
	}()
	log.Printf("serving %d ok ports from 50051, slow 50100, blackhole 50101, flapping 50102", *ok)
	select {}
}
