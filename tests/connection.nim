
suite "test suite for connections":

  test "connection public broker":
    let (tpc, msg) = tdata("connection public broker")

    proc conn() {.async.} =
      let ctx = newMqttCtx("nmqttTestConn" & tpc) # unique clientid for public broker
      ctx.setHost("broker-cn.emqx.io", 1883)
      await ctx.connect()
      await sleepAsync(1500)
      check(ctx.state == Connected)
      await ctx.publish(tpc, msg, 0)
      await sleepAsync(1500)
      await ctx.disconnect()
      check(ctx.state == Disabled)

    waitFor conn()

  test "connection public broker SSL":
    let (tpc, msg) = tdata("connection public broker SSL")

    proc conn() {.async.} =
      let ctx = newMqttCtx("nmqttTestConn" & tpc) # unique clientid for public broker
      ctx.setHost("broker-cn.emqx.io", 8883, true)
      await ctx.connect()
      await sleepAsync(1500)
      check(ctx.state == Connected)
      await ctx.publish(tpc, msg, 0)
      await sleepAsync(1500)
      await ctx.disconnect()
      check(ctx.state == Disabled)

    waitFor conn()

  test "connect() to broker":
    let ctxMain = newCtx()

    proc conn() {.async.} =
      await ctxMain.connect()
      await sleepAsync(500)
      check(ctxMain.state == Connected)

      # Do important stuff
      await sleepAsync(500)

      # Disconnect
      await ctxMain.disconnect()
      check(ctxMain.state == Disabled)

    waitFor conn()

  test "start() and reconnect":
    let ctxMain = newCtx()

    proc conn() {.async.} =
      await sleepAsync(500)
      check(ctxMain.state == Connected)

      # Do important stuff
      await sleepAsync(500)

      # Close connection
      ctxMain.state = Disconnecting
      ctxMain.s.close()
      await sleepAsync(500)

      # Auto-reconnect goes on `Disconnected"
      ctxMain.state = Disconnected
      # Auto-reconnect loop is 1000ms, wait 2000ms to ensure loop
      await sleepAsync(2000)

      # Check reconnect
      check(ctxMain.state == Connected)

      # Disconnect
      await ctxMain.disconnect()
      check(ctxMain.state == Disabled)

    waitFor conn()

  test "isConnected()":
    let ctxMain = newCtx()

    proc conn() {.async.} =
      check(ctxMain.isConnected() == false)
      await ctxMain.connect()
      await sleepAsync(500)
      check(ctxMain.isConnected() == true)

      await ctxMain.disconnect()
      check(ctxMain.state == Disabled)

    waitFor conn()
