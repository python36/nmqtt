suite "test suite for unsubscribe":

  test "unsubscribe from topic":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("unsubscribe from topic")

    proc conn() {.async.} =
      proc onDataUnsub(topic: string, message: string) =
        if topic == tpc:
          check(message == msg)
          return

      check(ctxListen.pubCallbacks.len() == 0)

      await ctxListen.subscribe(tpc, 0, onDataUnsub)

      check(ctxListen.pubCallbacks.len() == 1)

      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)

      check(ctxListen.pubCallbacks.len() == 0)

      await sleepAsync(500)
      await ctxMain.publish(tpc, "Msg must not be received", 0)
      await sleepAsync(500)

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Unsubscribe(02):",
                          "rx> Unsuback(00):",
                          "tx> Publish(00):"]))

    waitFor conn()


  test "unsubscribe from one of many topics":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc1, msg1) = tdata("unsubscribe from one of many topics")
      (tpc2, msg2) = tdata("unsubscribe from one of many topics")

    proc conn() {.async.} =
      var
        topic1: bool
        topic2: bool

      proc onDataUnsub1(topic: string, message: string) =
        check(message == msg1)
        topic1 = true

      proc onDataUnsub2(topic: string, message: string) =
        check(message == msg2)
        topic2 = true

      check(ctxListen.pubCallbacks.len() == 0)

      await ctxListen.subscribe(tpc1, 0, onDataUnsub1)
      await ctxListen.subscribe(tpc2, 0, onDataUnsub2)

      check(ctxListen.pubCallbacks.len() == 2)

      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc2)

      check(ctxListen.pubCallbacks.len() == 1)

      await sleepAsync(500)
      await ctxMain.publish(tpc1, msg1, 0)
      await ctxMain.publish(tpc2, msg2, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc1)

      await sleepAsync(500)
      check(ctxListen.pubCallbacks.len() == 0)

      check(topic1 == true)
      check(topic2 == false)

    waitFor conn()
