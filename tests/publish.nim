
suite "test suite for publish":

  test "publish multi line; json; long (757 chars); special chars":
    let
      ctxMain = newCtx()
      ctxListen = newCtx()
      (tpc1, _) = tdata("publish multi line")
      (tpc2, _) = tdata("publish json")
      (tpc3, _) = tdata("publish long (757 chars)")
      (tpc4, _) = tdata("publish special chars")

    const text1 = """1) If wishes were horses, beggars would ride.
    2) It’s easy to be wise after the event.
        3) Watch the doughnut, and not the hole.
            4) On a wing and a prayer"""

    const text2 = """
{
  "Novo": {
      "priceLatest": 359.35,
      "percentToday": -1.55,
      "plusminusToday": -5.65,
      "priceBuy": 359.35,
      "priceSell": 359.35,
      "priceHighest": 381.5,
      "priceLowest": 355.75,
      "tradeTotal": 6914045,
      "orderdepthBuy": 0,
      "orderdepthSell": 0,
      "epochtime": 1584771256,
      "success": true
  }
},
{
  "Alibaba": {
      "priceLatest": 180.35,
      "percentToday": -0.55,
      "plusminusToday": -8.65,
      "priceBuy": 181.25,
      "priceSell": 180.35,
      "priceHighest": 183.5,
      "priceLowest": 179.75,
      "tradeTotal": 13264045,
      "orderdepthBuy": 0,
      "orderdepthSell": 0,
      "epochtime": 1584771256,
      "success": true
  }
}"""

    const text3 = "Nim code specifies a computation that acts on a memory consisting of components called locations. A variable is basically a name for a location. Each variable and location is of a certain type. The variable's type is called static type, the location's type is called dynamic type. If the static type is not the same as the dynamic type, it is a super-type or subtype of the dynamic type. An identifier is a symbol declared as a name for a variable, type, procedure, etc. The region of the program over which a declaration applies is called the scope of the declaration. Scopes can be nested. The meaning of an identifier is determined by the smallest enclosing scope in which the identifier is declared unless overloading resolution rules suggest otherwise."

    const text4 = "*~\"%?+#!öôéè|§½';£@$ 诶艾弗艾儿豆贝尔维 НимИсТчеБест æøå αγλρξψ mənʊʃjõəd̪ʱɪkaːɾõ 😆😎😍😘"

    proc conn() {.async.} =
      var
        msgFound1: bool
        msgFound2: bool
        msgFound3: bool
        msgFound4: bool

      proc onDataPub1(topic: string, message: string) =
        msgFound1 = topic == tpc1 and message == text1

      proc onDataPub2(topic: string, message: string) =
        msgFound2 = topic == tpc2 and message == text2

      proc onDataPub3(topic: string, message: string) =
        msgFound3 = topic == tpc3 and message == text3

      proc onDataPub4(topic: string, message: string) =
        msgFound4 = topic == tpc4 and message == text4

      await ctxListen.subscribe(tpc1, 0, onDataPub1)
      await ctxListen.subscribe(tpc2, 0, onDataPub2)
      await ctxListen.subscribe(tpc3, 0, onDataPub3)
      await ctxListen.subscribe(tpc4, 0, onDataPub4)

      await sleepAsync(500)
      await ctxMain.publish(tpc1, text1, 0)
      await ctxMain.publish(tpc2, text2, 0)
      await ctxMain.publish(tpc3, text3, 0)
      await ctxMain.publish(tpc4, text4, 0)

      await sleepAsync(500)

      check(msgFound1 == true)
      check(msgFound2 == true)
      check(msgFound3 == true)
      check(msgFound4 == true)

    waitFor conn()
