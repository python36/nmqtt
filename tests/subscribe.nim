
suite "test suite for subscribe":

  test "subscribe to topic qos=0":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to topic qos=0")

    proc conn() {.async.} =
      var receivedMsg: bool

      proc onDataSubQoS0(topic: string, message: string) =
        if topic == tpc and message == msg:
          receivedMsg = true

      await ctxListen.subscribe(tpc, 0, onDataSubQoS0)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 0)
      await sleepAsync(500)

      check(receivedMsg == true)
      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(00):",
                          "rx> Publish(00):"]))

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to topic qos=1":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to topic qos=1")

    proc conn() {.async.} =
      var receivedMsg: bool

      proc onDataSubQoS1(topic: string, message: string) =
        if topic == tpc and message == msg:
          receivedMsg = true

      await ctxListen.subscribe(tpc, 1, onDataSubQoS1)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 1)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(receivedMsg == true)
      check(hasAllInDmp(@["tx> Subscribe(02):",
                          "rx> SubAck(00):",
                          "tx> Publish(02):",
                          "rx> PubAck(00):",
                          "rx> Publish(02):",
                          "tx> PubAck(02):"]))

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to topic qos=2":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to topic qos=2")

    proc conn() {.async.} =
      var receivedMsg: bool

      proc onDataSubQoS2(topic: string, message: string) =
        if topic == tpc and message == msg:
          receivedMsg = true

      await ctxListen.subscribe(tpc, 2, onDataSubQoS2)
      await sleepAsync(500)
      await ctxMain.publish(tpc, msg, 2)
      await sleepAsync(500)
      await ctxListen.unsubscribe(tpc)
      await sleepAsync(500)

      check(receivedMsg == true)
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
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to multiple topics":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to multiple topics")
      tpc1 = tpc & "-1"
      tpc2 = tpc & "-2"
      msg1 = msg & "-mul1"
      msg2 = msg & "-mul2"

    proc conn() {.async.} =
      var
        receivedMsg1: bool
        receivedMsg2: bool

      proc onDataSubMul1(topic: string, message: string) =
        if topic == tpc1 and message == msg1:
          receivedMsg1 = true

      proc onDataSubMul2(topic: string, message: string) =
        if topic == tpc2 and message == msg2:
          receivedMsg2 = true

      await ctxListen.subscribe(tpc1, 0, onDataSubMul1)
      await ctxListen.subscribe(tpc2, 0, onDataSubMul2)
      await sleepAsync(500)
      await ctxMain.publish(tpc1, msg1, 0)
      await ctxMain.publish(tpc2, msg2, 0)
      await sleepAsync(500)

      check(receivedMsg1 == true)
      check(receivedMsg2 == true)

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to multiple with identical topic":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to multiple with identical topic")

    proc conn() {.async.} =
      var sub1, sub2, sub3: int

      proc onDataSubMul1(topic: string, message: string) =
        if topic == tpc and message == msg:
          sub1 += 1

      proc onDataSubMul2(topic: string, message: string) =
        if topic == tpc and message == msg:
          sub2 += 1

      proc onDataSubMul3(topic: string, message: string) =
        if topic == tpc and message == msg:
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
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to #":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to #")
      tpc1 = tpc & "/random1"
      tpc2 = tpc & "/random2/1"
      tpc3 = tpc & "/random3/2/1/0"

    proc conn() {.async.} =
      var 
        receivedTpc1: bool
        receivedTpc2: bool
        receivedTpc3: bool

      proc onDataSubAll(topic: string, message: string) =
        if topic == tpc1 and message == msg:
          receivedTpc1 = true
        elif topic == tpc2 and message == msg:
          receivedTpc2 = true
        elif topic == tpc3 and message == msg:
          receivedTpc3 = true

      await ctxListen.subscribe("#", 0, onDataSubAll)
      await sleepAsync(500)
      check(receivedTpc1 == false)
      check(receivedTpc2 == false)
      check(receivedTpc3 == false)

      await ctxMain.publish(tpc1, msg, 0)
      await ctxMain.publish(tpc2, msg, 0)
      await ctxMain.publish(tpc3, msg, 0)
      await sleepAsync(500)

      check(receivedTpc1 == true)
      check(receivedTpc2 == true)
      check(receivedTpc3 == true)

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to test/#":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      ctxOther = newCtx()
      (tpc, msg) = tdata("subscribe to test/#")
      tpc1 = tpc & "/test/random1"
      tpc2 = tpc & "/test/"
      tpc3 = tpc & "/test"
      tpc4 = tpc & "/test/random3/2"

    proc conn() {.async.} =
      var 
        receivedTpc1: bool
        receivedTpc2: bool
        receivedTpc3: bool
        receivedTpc4: bool

      proc onDataSubWild(topic: string, message: string) =
        check(message == msg)
        if topic == tpc1:
          receivedTpc1 = true
        elif topic == tpc2:
          receivedTpc2 = true
        elif topic == tpc3:
          receivedTpc3 = true
        elif topic == tpc4:
          receivedTpc4 = true

      proc empty(topic: string, message: string) =
        discard

      await ctxListen.subscribe(tpc & "/test/#", 0, onDataSubWild)
      await ctxListen.subscribe(tpc & "/second/#", 0, empty)
      await ctxOther.subscribe(tpc & "/second/#", 0, empty)
      await sleepAsync(500)
      check(receivedTpc1 == false)
      check(receivedTpc2 == false)
      check(receivedTpc3 == false)
      check(receivedTpc4 == false)

      await ctxMain.publish(tpc1, msg, 0)
      await ctxMain.publish(tpc & "/second/random2", msg, 0)
      await ctxMain.publish(tpc2, msg, 0)
      await ctxMain.publish(tpc3, msg, 0)
      await ctxMain.publish(tpc4, msg, 0)
      await sleepAsync(500)

      check(receivedTpc1 == true)
      check(receivedTpc2 == true)
      check(receivedTpc3 == true)
      check(receivedTpc4 == true)

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()
    waitFor ctxOther.disconnect()

  test "subscribe to test/+":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/+")
      tpc1 = tpc & "/test/random1"
      tpc2 = tpc & "/test/"
      tpc3 = tpc & "/test/3"

    proc conn() {.async.} =
      var 
        receivedTpc1: bool
        receivedTpc2: bool
        receivedTpc3: bool

      proc onDataSubWild(topic: string, message: string) =
        check(message == msg)
        if topic == tpc1:
          receivedTpc1 = true
        elif topic == tpc2:
          receivedTpc2 = true
        elif topic == tpc3:
          receivedTpc3 = true

      await ctxListen.subscribe(tpc & "/test/+", 0, onDataSubWild)
      await sleepAsync(500)
      check(receivedTpc1 == false)
      check(receivedTpc2 == false)
      check(receivedTpc3 == false)

      await ctxMain.publish(tpc1, msg, 0)
      await ctxMain.publish(tpc & "/second/random2", msg, 0)
      await ctxMain.publish(tpc & "/test", msg, 0)
      await ctxMain.publish(tpc2, msg, 0)
      await ctxMain.publish(tpc3, msg, 0)
      await ctxMain.publish(tpc & "/test/random3/2", msg, 0)
      await sleepAsync(500)

      check(receivedTpc1 == true)
      check(receivedTpc2 == true)
      check(receivedTpc3 == true)

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to test/+/data":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/+/data")
      tpc1 = tpc & "test/1/data"
      tpc2 = tpc & "test/random4/data"
      tpc3 = tpc & "test//data"

    proc conn() {.async.} =
      var 
        receivedTpc1: bool
        receivedTpc2: bool
        receivedTpc3: bool

      proc onDataSubWild(topic: string, message: string) =
        check(message == msg)
        if topic == tpc1:
          receivedTpc1 = true
        elif topic == tpc2:
          receivedTpc2 = true
        elif topic == tpc3:
          receivedTpc3 = true

      await ctxListen.subscribe(tpc & "test/+/data", 0, onDataSubWild)
      await sleepAsync(500)
      check(receivedTpc1 == false)
      check(receivedTpc2 == false)
      check(receivedTpc3 == false)

      await ctxMain.publish(tpc1, msg, 0)
      await ctxMain.publish(tpc & "second/random2/data", msg, 0)
      await ctxMain.publish(tpc & "test/random3", msg, 0)
      await ctxMain.publish(tpc2, msg, 0)
      await ctxMain.publish(tpc3, msg, 0)
      await ctxMain.publish(tpc & "test/random5/data/random6", msg, 0)
      await sleepAsync(500)

      check(receivedTpc1 == true)
      check(receivedTpc2 == true)
      check(receivedTpc3 == true)

    waitFor conn()

  test "subscribe to test/+/+/data":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/+/+/data")
      tpc1 = tpc & "test/random5/random6/data"
      tpc2 = tpc & "test///data"
      tpc3 = tpc & "test/0/1/data"

    proc conn() {.async.} =
      var 
        receivedTpc1: bool
        receivedTpc2: bool
        receivedTpc3: bool

      proc onDataSubWild(topic: string, message: string) =
        check(message == msg)
        if topic == tpc1:
          receivedTpc1 = true
        elif topic == tpc2:
          receivedTpc2 = true
        elif topic == tpc3:
          receivedTpc3 = true

      await ctxListen.subscribe(tpc & "test/+/+/data", 0, onDataSubWild)
      await sleepAsync(500)
      check(receivedTpc1 == false)
      check(receivedTpc2 == false)
      check(receivedTpc3 == false)

      await ctxMain.publish(tpc & "test/random1/data", msg, 0)
      await ctxMain.publish(tpc & "second/random2/data", msg, 0)
      await ctxMain.publish(tpc & "test/random3", msg, 0)
      await ctxMain.publish(tpc & "test/random4/data", msg, 0)
      await ctxMain.publish(tpc & "test/random5/random6/data", msg, 0)
      await ctxMain.publish(tpc & "test///data", msg, 0)
      await ctxMain.publish(tpc & "test/random5/random6/random7/data", msg, 0)
      await ctxMain.publish(tpc & "test/random5/random6/data/random8", msg, 0)
      await ctxMain.publish(tpc & "test/0/1/data", msg, 0)
      await sleepAsync(500)

      check(receivedTpc1 == true)
      check(receivedTpc2 == true)
      check(receivedTpc3 == true)

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "subscribe to test/+/data/#":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("subscribe to test/+/data/#")
      tpc1 = tpc & "test//data"
      tpc2 = tpc & "test/random5/data/"
      tpc3 = tpc & "test/random5/data/random6/random7/random8"

    proc conn() {.async.} =
      var 
        receivedTpc1: bool
        receivedTpc2: bool
        receivedTpc3: bool

      proc onDataSubWild(topic: string, message: string) =
        check(message == msg)
        if topic == tpc1:
          receivedTpc1 = true
        elif topic == tpc2:
          receivedTpc2 = true
        elif topic == tpc3:
          receivedTpc3 = true

      await ctxListen.subscribe(tpc & "test/+/data/#", 0, onDataSubWild)
      await sleepAsync(500)
      check(receivedTpc1 == false)
      check(receivedTpc2 == false)
      check(receivedTpc3 == false)

      await ctxMain.publish(tpc & "test/random0/random1/data", msg, 0)
      await ctxMain.publish(tpc & "second/random2/data", msg, 0)
      await ctxMain.publish(tpc & "second/random2/data/test", msg, 0)
      await ctxMain.publish(tpc & "test/random3", msg, 0)
      await ctxMain.publish(tpc1, msg, 0)
      await ctxMain.publish(tpc2, msg, 0)
      await ctxMain.publish(tpc & "test/random5//data/random6/random7", msg, 0)
      await ctxMain.publish(tpc3, msg, 0)
      await ctxMain.publish(tpc & "random1/test/random5/data/random6/random7", msg, 0)
      await sleepAsync(500)

      check(receivedTpc1 == true)
      check(receivedTpc2 == true)
      check(receivedTpc3 == true)

    waitFor conn()
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "stay subscribed after disconnect with reconnect":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("stay subscribed after disconnect with reconnect")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubKeep(topic: string, message: string) =
        if topic == tpc and message == msg:
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
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "stay subscribed after disconnect with reconnect with same qos=2":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("stay subscribed after disconnect with reconnect with same qos=2")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubKeep(topic: string, message: string) =
        if topic == tpc and message == msg:
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
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

  test "stay subscribed after multipe (2) disconnect with reconnect":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("stay subscribed after multipe (2) disconnect with reconnect")

    proc conn() {.async.} =
      var msgCount: int

      proc onDataSubKeepMultiple(topic: string, message: string) =
        if topic == tpc and message == msg:
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
    waitFor ctxMain.disconnect()
    waitFor ctxListen.disconnect()

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
        if topic == tpc and message == msg:
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
    waitFor ctxMain.disconnect()
    waitFor ctxSlave.disconnect()
