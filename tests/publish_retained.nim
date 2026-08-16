
suite "test suite for publish retained":

  test "publish retain msg":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("publish retain msg")

    proc conn() {.async.} =
      var msgFound: bool

      waitFor ctxMain.publish(tpc, msg, qos=1, retain=true)
      waitFor sleepAsync(500)

      proc onDataRetain(topic: string, message: string) =
        if topic == tpc:
          check(message == msg)
          msgFound = true

      await ctxListen.subscribe(tpc, 2, onDataRetain)
      await sleepAsync(500)

      check(msgFound == true)

    waitFor conn()
