# AtomicDiskDrive

This is a solution to an assignment I got during a cuniversity course called "Distribute Systems".
The task was to implement a distributed block device, that is, a devce that stores data in a distributed register.
The solution was supposed to implement the following algorithm:

```
Implements:
    (N,N)-AtomicRegister instance nnar.

Uses:
    StubbornBestEffortBroadcast, instance sbeb;
    StubbornLinks, instance sl;

upon event < nnar, Init > do
    (ts, wr, val) := (0, 0, _);
    rid:= 0;
    readlist := [ _ ] `of length` N;
    acklist := [ _ ] `of length` N;
    reading := FALSE;
    writing := FALSE;
    writeval := _;
    readval := _;
    write_phase := FALSE;
    store(wr, ts, val, rid);

upon event < nnar, Recovery > do
    retrieve(wr, ts, val, rid);
    readlist := [ _ ] `of length` N;
    acklist := [ _ ]  `of length` N;
    reading := FALSE;
    readval := _;
    write_phase := FALSE;
    writing := FALSE;
    writeval := _;

upon event < nnar, Read > do
    rid := rid + 1;
    store(rid);
    readlist := [ _ ] `of length` N;
    acklist := [ _ ] `of length` N;
    reading := TRUE;
    trigger < sbeb, Broadcast | [READ_PROC, rid] >;

upon event < sbeb, Deliver | p [READ_PROC, r] > do
    trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;

upon event <sl, Deliver | q, [VALUE, r, ts', wr', v'] > such that r == rid and !write_phase do
    readlist[q] := (ts', wr', v');
    if #(readlist) > N / 2 and (reading or writing) then
        readlist[self] := (ts, wr, val);
        (maxts, rr, readval) := highest(readlist);
        readlist := [ _ ] `of length` N;
        acklist := [ _ ] `of length` N;
        write_phase := TRUE;
        if reading = TRUE then
            trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts, rr, readval] >;
        else
            (ts, wr, val) := (maxts + 1, rank(self), writeval);
            store(ts, wr, val);
            trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts + 1, rank(self), writeval] >;

upon event < nnar, Write | v > do
    rid := rid + 1;
    writeval := v;
    acklist := [ _ ] `of length` N;
    readlist := [ _ ] `of length` N;
    writing := TRUE;
    store(rid);
    trigger < sbeb, Broadcast | [READ_PROC, rid] >;

upon event < sbeb, Deliver | p, [WRITE_PROC, r, ts', wr', v'] > do
    if (ts', wr') > (ts, wr) then
        (ts, wr, val) := (ts', wr', v');
        store(ts, wr, val);
    trigger < sl, Send | p, [ACK, r] >;

upon event < sl, Deliver | q, [ACK, r] > such that r == rid and write_phase do
    acklist[q] := Ack;
    if #(acklist) > N / 2 and (reading or writing) then
        acklist := [ _ ] `of length` N;
        write_phase := FALSE;
        if reading = TRUE then
            reading := FALSE;
            trigger < nnar, ReadReturn | readval >;
        else
            writing := FALSE;
            trigger < nnar, WriteReturn >;
```

The picture `atdd.svg` describes the architecture of the system.

DISCLAIMER!
A template for the solution to this assignment was provided, it contained definitions of traits, that the components of this system were supposed to implement.
Thus my work consisted mostly of implementing theese traits. Morover anything outside the `solution` folder was proveded with the template.
See commit "First Commit" to see the template.
