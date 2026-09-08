
suite "test suite for publish retained":

  test "publish retain msg":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      ctxNotEmpty = newCtx()
      ctxEmpty = newCtx()
      (tpc, msg) = tdata("publish retain msg")

    proc conn() {.async.} =
      var
        msgFound: bool
        msgFound2: bool
        msgFound3: bool

      waitFor ctxMain.publish(tpc, msg, retain=true)
      waitFor sleepAsync(500)

      proc onDataRetain(topic: string, message: string) =
        if topic == tpc and message == msg:
          check(msgFound == false)
          msgFound = true

      proc onDataRetain2(topic: string, message: string) =
        if topic == tpc and message == msg:
          check(msgFound2 == false)
          msgFound2 = true

      proc onDataRetain3(topic: string, message: string) =
        if topic == tpc and message == msg:
          check(msgFound3 == false)
          msgFound3 = true

      await ctxListen.subscribe(tpc, 2, onDataRetain)
      await sleepAsync(500)

      waitFor ctxMain.publish(tpc, "")
      await sleepAsync(500)
      await ctxNotEmpty.subscribe(tpc, 2, onDataRetain2)
      await sleepAsync(500)

      waitFor ctxMain.publish(tpc, "", retain=true)
      await sleepAsync(500)
      await ctxEmpty.subscribe(tpc, 2, onDataRetain3)
      await sleepAsync(500)

      check(msgFound == true)
      check(msgFound2 == true)
      check(msgFound3 == false)

    waitFor conn()

  test "subscribe retain msg":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc, msg) = tdata("publish retain msg")

      tpc1 = tpc & "/test/"
      tpc2 = tpc & "/test"
      tpc3 = tpc & "//data"
      tpc4 = tpc & "/test/data"
      tpc5 = tpc & "/test/random"
      tpc6 = tpc & "/test/random/1/2/3"
      tpc7 = tpc & "/test/data/"
      tpc8 = tpc & "test/"

    proc conn() {.async.} =
      var
        msgFound1: int
        msgFound2: int
        msgFound3: int
        msgFound4: int

      waitFor ctxMain.publish(tpc1, msg, retain=true)
      waitFor ctxMain.publish(tpc2, msg, retain=true)
      waitFor ctxMain.publish(tpc3, msg, retain=true)
      waitFor ctxMain.publish(tpc4, msg, retain=true)
      waitFor ctxMain.publish(tpc5, msg, retain=true)
      waitFor ctxMain.publish(tpc6, msg, retain=true)
      waitFor ctxMain.publish(tpc7, msg, retain=true)
      waitFor ctxMain.publish(tpc8, msg, retain=true)

      waitFor sleepAsync(500)

      proc onDataRetain1(topic: string, message: string) =
        if topic == tpc1 and message == msg:
          inc msgFound1

      proc onDataRetain2(topic: string, message: string) =
        if message == msg:
          inc msgFound2

      proc onDataRetain3(topic: string, message: string) =
        if topic == tpc3 and message == msg:
          inc msgFound3

      proc onDataRetain4(topic: string, message: string) =
        if topic == tpc6 and message == msg:
          inc msgFound4

      await ctxListen.subscribe(tpc1, 0, onDataRetain1)
      await sleepAsync(500)
      await ctxListen.subscribe(tpc & "/+/data", 0, onDataRetain3)
      await sleepAsync(500)
      await ctxListen.subscribe(tpc & "/+/random/1/#", 0, onDataRetain4)
      await sleepAsync(500)
      await ctxListen.subscribe(tpc & "/#", 0, onDataRetain2)
      await sleepAsync(500)

      # Expected count is 2 due to MQTT Retained message behavior:
      # Each message is received twice because it is triggered first by its 
      # own specific subscription, and then a second time by the wildcard `/#` 
      # subscription, which forces the broker to resend all retained data.
      check(msgFound1 == 2)
      check(msgFound3 == 2)
      check(msgFound4 == 2)
      check(msgFound2 == 7)

      waitFor ctxMain.publish(tpc1, "", retain=true)
      waitFor ctxMain.publish(tpc2, "", retain=true)
      waitFor ctxMain.publish(tpc3, "", retain=true)
      waitFor ctxMain.publish(tpc4, "", retain=true)
      waitFor ctxMain.publish(tpc5, "", retain=true)
      waitFor ctxMain.publish(tpc6, "", retain=true)
      waitFor ctxMain.publish(tpc7, "", retain=true)
      waitFor ctxMain.publish(tpc8, "", retain=true)

      await sleepAsync(500)

    waitFor conn()
