
suite "test suite for ping":

  test "set ping interval":
    let
      ctxMain = newCtx()
      (tpc, msg) = tdata("set ping interval")

    proc conn() {.async.} =
      var
        pingCount: int
        pingResp: int

      proc empty(topic: string, message: string) =
        discard

      proc findPings() =
        for i in testDmp:
          if i[0] == "tx> PingReq(00):": pingCount += 1
          if i[0] == "rx> PingResp(00):": pingResp += 1

      ctxMain.setPingInterval(1)
      await ctxMain.connect()

      for i in 1 .. 4:
        await sleepAsync(500)
        await ctxMain.subscribe(tpc, 0, empty)
        await sleepAsync(500)
        await ctxMain.publish(tpc, msg, 0)
        await sleepAsync(500)
        await ctxMain.unsubscribe(tpc)

      findPings()
      checkpoint("Ping only if no other messages were sent")
      check(pingCount == 0)
      check(pingCount == 0)

      await sleepAsync(6000)

      for i in testDmp:
        if i[0] == "tx> PingReq(00):": pingCount += 1
        if i[0] == "rx> PingResp(00):": pingResp += 1

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
      for i in testDmp:
        if i[0] == "tx> PingReq(00):": pingCount += 1
        if i[0] == "rx> PingResp(00):": pingResp += 1

      checkpoint("Ping with 60 second interval during 6 seconds")
      check(pingCount == 0)
      check(pingResp == 0)

    waitFor conn()
