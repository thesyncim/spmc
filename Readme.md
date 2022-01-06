SPMC

Generic lock free single producer multiple consumer (adapted from sync.Pool)


``` 
goos: darwin
goarch: amd64
pkg: github.com/thesyncim/spmc
cpu: Intel(R) Core(TM) i9-9880H CPU @ 2.30GHz
BenchmarkChannelSPMC
BenchmarkChannelSPMC-16    	 2788314	       491.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkSPMC
BenchmarkSPMC-16           	1000000000	         0.9714 ns/op	       0 B/op	       0 allocs/op
PASS
```
