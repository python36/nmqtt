const msgCount = 500

suite "test suite for publish with qos":

  test "publish multiple message fast qos=0,1,2":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc0, _) = tdata("publish multiple message fast qos=0")
      (tpc1, _) = tdata("publish multiple message fast qos=1")
      (tpc2, _) = tdata("publish multiple message fast qos=2")

    proc conn() {.async.} =
      await sleepAsync(500)

      var
        msgs0: array[msgCount, bool]
        msgs1: array[msgCount, bool]
        msgs2: array[msgCount, bool]
        receivedAllMsgs0: bool
        receivedAllMsgs1: bool
        receivedAllMsgs2: bool

      proc checkMsgs(msgs: array[msgCount, bool]): bool =
        for m in msgs:
          if not m:
            return false
        true

      proc onDataQoS0(topic: string, message: string) =
        let i = parseInt(message)
        check(i in 0 .. msgCount - 1)
        msgs0[i] = true

      proc onDataQoS1(topic: string, message: string) =
        let i = parseInt(message)
        check(i in 0 .. msgCount - 1)
        msgs1[i] = true

      proc onDataQoS2(topic: string, message: string) =
        let i = parseInt(message)
        check(i in 0 .. msgCount - 1)
        msgs2[i] = true

      await ctxListen.subscribe(tpc0, 0, onDataQoS0)
      await ctxListen.subscribe(tpc1, 0, onDataQoS1)
      await ctxListen.subscribe(tpc2, 0, onDataQoS2)

      for i in 0 ..< msgCount:
        await ctxMain.publish(tpc0, $i, 0)
        await ctxMain.publish(tpc1, $i, 1)
        await ctxMain.publish(tpc2, $i, 2)

      check(ctxMain.state == Connected)

      for i in 0 .. 9:
        await sleepAsync(500)

        if not receivedAllMsgs0:
          receivedAllMsgs0 = checkMsgs(msgs0)
        if not receivedAllMsgs1:
          receivedAllMsgs1 = checkMsgs(msgs1)
        if not receivedAllMsgs2:
          receivedAllMsgs2 = checkMsgs(msgs2)

        if receivedAllMsgs0 and receivedAllMsgs1 and receivedAllMsgs2:
          break

      check(receivedAllMsgs0 == true)
      check(receivedAllMsgs1 == true)
      check(receivedAllMsgs2 == true)

      var hasQueue: bool
      for w in ctxMain.workQueue.values():
        if w.typ != PingReq:
          hasQueue = true
          break
      check(hasQueue == false)

    waitFor conn()
