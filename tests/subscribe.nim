
suite "test suite for subscribe":

  test "subscribe to topic qos=0":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to topic qos=0")

    proc conn() {.async.} =
      proc onDataSubQoS0(topic: string, message: string) =
        if topic == tpc:
          check(message == msg)
          return
      await ctxListen.subscribe(tpc, 0, onDataSubQoS0)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):"]))

    waitFor conn()

  test "subscribe to topic qos=1":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to topic qos=1")

    proc conn() {.async.} =
      proc onDataSubQoS1(topic: string, message: string) =
        if topic == tpc:
          check(message == msg)
          return

      await ctxListen.subscribe(tpc, 1, onDataSubQoS1)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 1)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(02):",
                          "rx> PubAck(00):",
                          "rx> Publish(02):",
                          "tx> PubAck(02):"]))

    waitFor conn()

  test "subscribe to topic qos=2":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to topic qos=2")

    proc conn() {.async.} =
      proc onDataSubQoS2(topic: string, message: string) =
        if topic == tpc:
          check(message == msg)
          return

      await ctxListen.subscribe(tpc, 2, onDataSubQoS2)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 2)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(04):",
                          "rx> PubRec(00):",
                          "tx> PubRel(02):",
                          "rx> PubComp(00):",
                          "rx> Publish(04):",
                          "tx> PubRec(02):",
                          "rx> PubRel(02):",
                          "tx> PubComp(02):"]))

    waitFor conn()

  test "subscribe to multiple topics":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to multiple topics")

    proc conn() {.async.} =
      var topic1, topic2: bool

      proc onDataSubMul1(topic: string, message: string) =
        check(message == msg & "-mul1")
        check(not topic1)
        topic1 = true

      proc onDataSubMul2(topic: string, message: string) =
        check(message == msg & "-mul2")
        check(not topic2)
        topic2 = true

      await ctxListen.subscribe(tpc & "-1", 0, onDataSubMul1)
      await ctxListen.subscribe(tpc & "-2", 0, onDataSubMul2)
      await sleepAsync(500)
      await ctxMain.publish(tpc & "-1", msg & "-mul1", 0)
      await ctxMain.publish(tpc & "-2", msg & "-mul2", 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc & "-1")
      await ctxListen.unsubscribe(tpc & "-2")

      check(topic1)
      check(topic2)

    waitFor conn()

  test "subscribe to multiple with identical topic":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to multiple with identical topic")

    proc conn() {.async.} =
      var sub1, sub2, sub3: int

      proc onDataSubMul1(topic: string, message: string) =
        if topic == tpc:
          sub1 += 1

      proc onDataSubMul2(topic: string, message: string) =
        if topic == tpc:
          sub2 += 1

      proc onDataSubMul3(topic: string, message: string) =
        if topic == tpc:
          sub3 += 1

      check(ctxListen.pubCallbacks.len() == 0)

      # Add 1 msg to sub1
      await ctxListen.subscribe(tpc, 0, onDataSubMul1)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0)

      check(ctxListen.pubCallbacks.len() == 1)

      await sleepAsync(500)
      await ctxListen.subscribe(tpc, 0, onDataSubMul2)

      # sub3 now overrides sub1 and sub2
      await ctxListen.subscribe(tpc, 0, onDataSubMul3)

      check(ctxListen.pubCallbacks.len() == 1)

      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0)
      await ctxMain.publish(tpc, msg, 0)
      await ctxMain.publish(tpc, msg, 0)
      await sleepAsync(500)

      await ctxListen.unsubscribe(tpc)
      check(ctxListen.pubCallbacks.len() == 0)

      check(sub1 == 1)
      check(sub2 == 0)
      check(sub3 == 3)

    waitFor conn()

  test "subscribe to #":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to #")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubAll(topic: string, message: string) =
         msgCount += 1

      await ctxListen.subscribe(tpc & "/#", 0, onDataSubAll)
      await sleepAsync(500)
      await ctxMain.publish(tpc & "/random1", msg, 0)
      await ctxMain.publish(tpc & "/random2/1", msg, 0)
      await ctxMain.publish(tpc & "/random3/2/1/0", msg, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc & "/#")

      check(msgCount == 3)

    waitFor conn()

  test "subscribe to test/#":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/#")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubWild(topic: string, message: string) =
        msgCount += 1

      await ctxListen.subscribe(tpc & "/test/#", 0, onDataSubWild)
      await sleepAsync(500)
      await ctxMain.publish(tpc & "/test/random1", msg, 0)
      await ctxMain.publish(tpc & "/second/random2", msg, 0)
      await ctxMain.publish(tpc & "/test", msg, 0)
      await ctxMain.publish(tpc & "/test/random3/2", msg, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc & "/test/#")

      check(msgCount == 3)

    waitFor conn()

  test "subscribe to test/+":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/+")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubWild(topic: string, message: string) =
        msgCount += 1

      await ctxListen.subscribe(tpc & "/test/+", 0, onDataSubWild)
      await sleepAsync(500)
      await ctxMain.publish(tpc & "/test/random1", msg, 0)
      await ctxMain.publish(tpc & "/second/random2", msg, 0)
      await ctxMain.publish(tpc & "/test", msg, 0)
      await ctxMain.publish(tpc & "/test/random3", msg, 0)
      await ctxMain.publish(tpc & "/test/random3/2", msg, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc & "/test/+")
      check(msgCount == 2)

    waitFor conn()

  test "subscribe to test/+/test":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/+/test")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubWild(topic: string, message: string) =
        msgCount += 1

      await ctxListen.subscribe(tpc & "test/+/data", 0, onDataSubWild)
      await sleepAsync(500)
      await ctxMain.publish(tpc & "test/random1/data", msg, 0)
      await ctxMain.publish(tpc & "second/random2/data", msg, 0)
      await ctxMain.publish(tpc & "test/random3", msg, 0)
      await ctxMain.publish(tpc & "test/random4/data", msg, 0)
      await ctxMain.publish(tpc & "test/random5/data/random6", msg, 0)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc & "test/+/data")
      check(msgCount == 2)

    waitFor conn()

  test "stay subscribed after disconnect with reconnect":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("stay subscribed after disconnect with reconnect")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubKeep(topic: string, message: string) =
        msgCount += 1

      await ctxListen.subscribe(tpc, 0, onDataSubKeep)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0) # msg 1
      await sleepAsync(500)

      # Disconnect
      ctxListen.state = Disconnecting
      ctxListen.s.close()
      await sleepAsync(500)
      ctxListen.state = Disconnected
      await sleepAsync(2000) # Auto-reconnect loop is 1000ms, wait 2000ms to ensure loop
      # We should automatic reconnect here

      await ctxMain.publish(tpc, msg, 0) # msg 2
      await ctxMain.publish(tpc, msg, 0) # msg 3
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(msgCount == 3) # A total of 3 msgs on this topic

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Connect(00):",
                          "rx> ConnAck(00):",
                          "tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Unsubscribe(02):",
                          "rx> Unsuback(00):"]))

      await ctxListen.disconnect()

    waitFor conn()

  test "stay subscribed after disconnect with reconnect with same qos=2":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("stay subscribed after disconnect with reconnect with same qos=2")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubKeep(topic: string, message: string) =
        msgCount += 1

      await ctxListen.subscribe(tpc, 2, onDataSubKeep)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0) # msg 1
      await sleepAsync(500)

      # Disconnect
      ctxListen.state = Disconnecting
      ctxListen.s.close()
      await sleepAsync(500)
      ctxListen.state = Disconnected
      await sleepAsync(2000) # Auto-reconnect loop is 1000ms, wait 2000ms to ensure loop
      # We should automatic reconnect here

      await ctxMain.publish(tpc, msg, 2) # msg 2
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(msgCount == 2) # A total of 2 msgs on this topic

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Connect(00):",
                          "rx> ConnAck(00):",
                          "tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(04):",
                          "rx> PubRec(00):",
                          "tx> PubRel(02):",
                          "rx> PubComp(00):",
                          "rx> Publish(04):",
                          "tx> PubRec(02):",
                          "rx> PubRel(02):",
                          "tx> PubComp(02):",
                          "tx> Unsubscribe(02):",
                          "rx> Unsuback(00):"]))

    waitFor conn()

  test "stay subscribed after multipe (2) disconnect with reconnect":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("stay subscribed after multipe (2) disconnect with reconnect")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubKeepMultiple(topic: string, message: string) =
        msgCount += 1

      await ctxListen.subscribe(tpc, 0, onDataSubKeepMultiple)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0) # msg 1
      await sleepAsync(500)

      # Disconnect 1/2
      ctxListen.state = Disconnecting
      ctxListen.s.close()
      await sleepAsync(500)
      ctxListen.state = Disconnected
      await sleepAsync(2000) # Auto-reconnect loop is 1000ms, wait 2000ms to ensure loop
      # We should automatic reconnect here

      # Disconnect 2/2
      ctxListen.state = Disconnecting
      ctxListen.s.close()
      await sleepAsync(500)
      ctxListen.state = Disconnected
      await sleepAsync(2000) # Auto-reconnect loop is 1000ms, wait 2000ms to ensure loop
      # We should automatic reconnect here

      await ctxMain.publish(tpc, msg, 0) # msg 2
      await ctxMain.publish(tpc, msg, 0) # msg 3
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(msgCount == 3) # A total of 3 msgs on this topic

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Connect(00):",
                          "rx> ConnAck(00):",
                          "tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Connect(00):",
                          "rx> ConnAck(00):",
                          "tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Unsubscribe(02):",
                          "rx> Unsuback(00):"]))

    waitFor conn()

  test "stay subscribed after long disconnect with reconnect":
    ## This test currently needs manual actions - you need to close/disconnect
    ## your broker during the test and open/reconnect.

    echo "\n\nTHIS TEST NEEDS MANUAL ACTIONS - STAY READY\n\n"

    let
      ctxMain = newCtx()
      ctxSlave = newCtx()
      (tpc, msg) = tdata("stay subscribed after long disconnect with reconnect")

    proc conn() {.async.} =
      ctxSlave.setPingInterval(90) # Increas ping to avoid interference with package order

      var msgCount: int
      proc onDataSubKeepLong(topic: string, message: string) =
        msgCount += 1

      await ctxSlave.subscribe(tpc, 0, onDataSubKeepLong)
      await sleepAsync(500)
      await ctxSlave.publish(tpc, msg, 0) # msg 1

      # Disconnect
      echo "\n\nDISCONNECT THE BROKER NOW (you have 5 sec)\n\n"
      await sleepAsync(5000)

      # Publish messages while the broker is down
      await ctxSlave.publish(tpc, msg, 0) # msg 2
      await ctxSlave.publish(tpc, msg, 0) # msg 3
      await ctxSlave.publish(tpc, msg, 0) # msg 4
      await ctxSlave.publish(tpc, msg, 0) # msg 5
      await sleepAsync(5000)

      # Reconnect the broker
      echo "\n\nCONNECT THE BROKER NOW (you have 5 sec)\n\n"
      await sleepAsync(5000)

      # Send messages when the broker is up again
      await ctxSlave.publish(tpc, msg, 0) # msg 2
      await ctxSlave.publish(tpc, msg, 0) # msg 3
      await sleepAsync(500)
      await ctxSlave.unsubscribe(tpc)
      await sleepAsync(500)

      check(msgCount == 7)

      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Disconnect(00):",
                          "tx> Connect(00):",
                          "rx> ConnAck(00):",
                          "tx> Subscribe(02):",
                          "tx> Publish(00):",
                          "tx> Publish(00):",
                          "tx> Publish(00):",
                          "tx> Publish(00):",
                          "rx> SubAck(00):",
                          "rx> Publish(00):",
                          "rx> Publish(00):",
                          "rx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Publish(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):",
                          "rx> Publish(00):",
                          "tx> Unsubscribe(02):",
                          "rx> Unsuback(00):"]))

    waitFor conn()
