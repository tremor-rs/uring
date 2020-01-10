```text


transport (WS, REST)
  has: SwitchInboundChannelSender
  has: SwitchOutboundChannelReceiver
  has: SwitchOutboundChannelSender



^-(channel = request + client-id + correleation-id (SwitchInboundChanell) / reply + client-id + correleation-id (SwitchOutboundChannel) )-v

Protocol-Switch (Connect, Select)
  has: SwitchInboundChannelReceiver

^-(channel to sub handler of {"request": <binary>, "request-id": <id>} / {"reply": <binary>, "request-id": <id>})-v

 Sub-Protocosl-Handling

```


