
suite "test suite for will messages":

  test "send will msg with default values":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, _) = tdata("send will msg with default values")

    proc conn() {.async.} =

      const willMsg = "willmsg_qos0_retain=false"
      var willMsgCheck: bool

      proc onDataWill(topic: string, message: string) =
        if topic == tpc and message == willMsg:
          willMsgCheck = true

      await ctxListen.subscribe(tpc, 2, onDataWill)

      ctxMain.setWill(tpc, willMsg)
      await ctxMain.connect()
      await sleepAsync(500) # Wait for full connection
      ctxMain.s.close()
      await sleepAsync(500) # Wait for willMsg to be sent

      check(willMsgCheck == true)

    waitFor conn()

  test "send will msg retained = true":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      ctxDestroy = newCtx()
      (tpc, _) = tdata("send will msg retained = true")

    proc conn() {.async.} =

      const willMsg = "willmsg_qos0_retain=true"
      var
        willMsgCheck: bool
        willMsgRetain: bool

      proc onDataWill(topic: string, message: string) =
        if topic == tpc and message == willMsg:
          willMsgCheck = true

      await ctxListen.subscribe(tpc, 2, onDataWill)

      # Set will and send
      ctxMain.setWill(tpc, willMsg, retain=true)
      await ctxMain.connect()
      await sleepAsync(500) # Wait for full connection
      ctxMain.s.close()
      await sleepAsync(500) # Wait for willMsg to be sent

      check(willMsgCheck == true)

      proc onDataWillRetain(topic: string, message: string) =
        if topic == tpc and message == willMsg:
          willMsgRetain = true

      await ctxDestroy.subscribe(tpc, 2, onDataWillRetain)
      await sleepAsync(500)

      check(willMsgRetain == true)

    waitFor conn()
