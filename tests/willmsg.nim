
suite "test suite for will messages":

  test "send will msg with default values":
    let
      ctxMain = newCtx(autoStart = false)
      ctxListen = newCtx()
      (tpc, _) = tdata("send will msg with default values")

    proc conn() {.async.} =

      const willMsg = "willmsg_qos0_retain=false"
      var willMsgCheck: int

      proc onDataWill(topic: string, message: string) =
        if topic == tpc and message == willMsg:
          check(willMsgCheck == 0)
          inc willMsgCheck

      await ctxListen.subscribe(tpc, 2, onDataWill)
      await sleepAsync(500)
      check(willMsgCheck == 0)

      ctxMain.setWill(tpc, willMsg)

      await ctxMain.start()
      await sleepAsync(500)
      await ctxMain.disconnect()
      await sleepAsync(500)

      check(willMsgCheck == 0)

      await ctxMain.start()
      await sleepAsync(500)
      ctxMain.s.close()
      await sleepAsync(500)

      check(willMsgCheck == 1)

    waitFor conn()

  test "send will msg retained = true,false":
    let
      ctxRetain = newCtx(autoStart = false)
      ctxNotRetain = newCtx(autoStart = false)
      ctxListen = newCtx()
      ctxDestroy = newCtx()
      (tpc1, _) = tdata("send will msg retained = true")
      (tpc2, _) = tdata("send will msg retained = false")

    proc conn() {.async.} =
      const willMsgRetain = "willmsg_qos0_retain=true"
      const willMsgNotRetain = "willmsg_qos0_retain=false"
      var
        willMsgRetainReceived: bool
        willMsgNotRetainReceived: bool
        willMsgRetained: bool
        willMsgNotRetained: bool

      proc onDataWill(topic: string, message: string) =
        if topic == tpc1 and message == willMsgRetain:
          willMsgRetainReceived = true
        elif topic == tpc2 and message == willMsgNotRetain:
          willMsgNotRetainReceived = true

      await ctxListen.subscribe(tpc1, 2, onDataWill)
      await ctxListen.subscribe(tpc2, 2, onDataWill)

      # Set will and send
      ctxRetain.setWill(tpc1, willMsgRetain, retain=true)
      ctxNotRetain.setWill(tpc2, willMsgNotRetain, retain=false)
      await ctxRetain.start()
      await ctxNotRetain.start()
      await sleepAsync(500)
      check(willMsgRetainReceived == false)
      check(willMsgNotRetainReceived == false)
      ctxRetain.s.close()
      ctxNotRetain.s.close()
      await sleepAsync(500)

      check(willMsgRetainReceived == true)
      check(willMsgNotRetainReceived == true)

      proc onDataWillRetain(topic: string, message: string) =
        if topic == tpc1 and message == willMsgRetain:
          willMsgRetained = true
        elif topic == tpc2 and message == willMsgNotRetain:
          willMsgNotRetained = true

      await ctxDestroy.subscribe(tpc1, 2, onDataWillRetain)
      await ctxDestroy.subscribe(tpc2, 2, onDataWillRetain)
      await sleepAsync(500)

      check(willMsgRetained == true)
      check(willMsgNotRetained == false)

    waitFor conn()
