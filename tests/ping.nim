
suite "test suite for ping":

  test "set ping interval":
    let
      ctxMain = newCtx()
      (tpc, msg) = tdata("set ping interval")

    proc conn() {.async.} =

      ctxMain.setPingInterval(1)
      await ctxMain.connect()
      await sleepAsync(6000)

      var
        pingCount: int
        pingResp: int
      for ping in testDmp:
        if ping[0] == "tx> PingReq(00):": pingCount += 1
        if ping[0] == "rx> PingResp(00):": pingResp += 1

      checkpoint("Ping with 1 second interval during 6 seconds")
      check(pingCount > 3)
      check(pingResp > 3)

      await ctxMain.disconnect()
      await sleepAsync(500)

      testDmp = @[]
      ctxMain.setPingInterval(60)
      await ctxMain.connect()
      await sleepAsync(6000)

      pingCount = 0
      pingResp = 0
      for ping in testDmp:
        if ping[0] == "tx> PingReq(00):": pingCount += 1
        if ping[0] == "rx> PingResp(00):": pingResp += 1

      checkpoint("Ping with 60 second interval during 6 seconds")
      check(pingCount == 0)
      check(pingResp == 0)

    waitFor conn()