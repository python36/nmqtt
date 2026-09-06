import asyncdispatch, unittest, oids, random

var testDmp: seq[seq[string]]

when not defined(test):
  echo "Please run with -d:test, exiting"
  quit()

include "../nmqtt.nim"

randomize()

proc newCtx(autoStart: bool = true): MqttCtx =
  result = newMqttCtx("nmqttTest-" & $genOid())
  result.setHost("127.0.0.1", 1883)
  if autoStart:
    waitFor result.start()

proc tout(t, m, s: string) =
  ## Print test data during test.
  echo "  \e[17m" & t & " - " & m & " - " & s & "\e[0m\n"

proc hasAllInDmp(s: seq[string]): bool =
  var t = s
  for rec in testDmp:
    var
      j = 0
    while j < len(t):
      if rec[0] == t[j]:
        t.del(j)
        break
      inc j
  len(t) == 0

proc tdata(t: string): (string, string) =
  ## Generate the test topic and message
  let topicTest = $genOid()
  let msg = $rand(99999999)
  tout(topicTest, msg, t)
  testDmp = @[]
  return (topicTest, msg)

include "connection.nim"
include "subscribe.nim"
include "unsubscribe.nim"
include "publish.nim"
include "publish_qos.nim"
include "ping.nim"
include "tools.nim"
include "willmsg.nim"          # Contains retained msgs
include "publish_retained.nim" # Contains retained msgs
                               #
                               # When the test contains retained msgs,
                               # it needs to be the last test, since it
                               # will store msg's, which will be caught
                               # in the subscribe test on `#`.
