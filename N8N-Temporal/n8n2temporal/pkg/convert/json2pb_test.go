package convert

import (
	"context"
	"testing"
)

func TestLoadPbDescriptor(t *testing.T) {
	j := NewJsonPB("https://github.acme.red/intelli-sec/idl/blob/main/proto/task/v1/task.proto", "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", []string{"dir_brute_task_result.url"})
	var bd = `{
            "dir_brute_task_result": [
                {
                    "response": {
                        "body": "W3sKICAicmVsYXRpb24iOiBbImRlbGVnYXRlX3Blcm1pc3Npb24vY29tbW9uLmhhbmRsZV9hbGxfdXJscyJdLAogICJ0YXJnZXQiOiB7CiAgICAibmFtZXNwYWNlIjogImFuZHJvaWRfYXBwIiwKICAgICJwYWNrYWdlX25hbWUiOiAidHYuZGFubWFrdS5iaWxpIiwKICAgICJzaGEyNTZfY2VydF9maW5nZXJwcmludHMiOgogICAgWyI5MzpCQToyNzowRjo1NToyMToxMzo5RTpDQTpGRTo0QjpCNjozODpBQzo1QjoxMTo5ODpCQzo1NDo4Rjo2MjpEOTpGRDo4Rjo4NTo4MDpBMDo3OTpGQTpGNTo5MTowRSJdCiAgfQp9XQ==",
                        "headers": [
                            {
                                "key": "Date",
                                "value": "Mon, 24 Nov 2025 08:57:04 GMT"
                            },
                            {
                                "key": "Content-Type",
                                "value": "application/json"
                            },
                            {
                                "key": "Server",
                                "value": "openresty"
                            },
                            {
                                "key": "Cache-Control",
                                "value": "no-cache"
                            },
                            {
                                "key": "X-Cache-Webcdn",
                                "value": "MISS from blzone06"
                            },
                            {
                                "key": "X-Save-Date",
                                "value": "Mon, 24 Nov 2025 08:57:04 GMT"
                            },
                            {
                                "key": "Content-Length",
                                "value": "292"
                            },
                            {
                                "key": "Vary",
                                "value": "Origin,Accept-Encoding"
                            },
                            {
                                "key": "Expires",
                                "value": "Mon, 24 Nov 2025 08:57:03 GMT"
                            },
                            {
                                "key": "X-Cache-Time",
                                "value": "0"
                            }
                        ],
                        "reason": "OK",
                        "status": 200
                    },
                    "url": "http://bilibili.com/.well-known/assetlinks.json"
                },
                {
                    "response": {
                        "body": "PCFET0NUWVBFIGh0bWw+PGh0bWwgbGFuZz0iZW4iPjxoZWFkPjxtZXRhIGNoYXJzZXQ9IlVURi04Ij48bWV0YSBuYW1lPSJ2aWV3cG9ydCIgY29udGVudD0id2lkdGg9ZGV2aWNlLXdpZHRoLGluaXRpYWwtc2NhbGU9MSI+PHRpdGxlPkJJTElCSUxJIDEx5ZGo5bm05ryU6K6yPC90aXRsZT48bWV0YSBuYW1lPSJkZXNjcmlwdGlvbiIgY29udGVudD0iYmlsaWJpbGnljYHkuIDlsoHnlJ/ml6XvvIzmiJHku6zpgoDor7fkuobmlbDkvY3lmInlrr7lnKgxMeWRqOW5tOa8lOiusuS8mueOsOWcuuWIhuS6q+S7luS7rOS4jkLnq5nnmoTmlYXkuovjgII25pyIMjbml6UxOTowMO+8jOmUgeWumuKAnOWTlOWTqeWTlOWTqeW8ueW5lee9keKAneebtOaSremXtOOAgiI+PG1ldGEgbmFtZT0idmlld3BvcnQiIGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCxpbml0aWFsLXNjYWxlPTEsbWluaW11bS1zY2FsZT0xLG1heGltdW0tc2NhbGU9MSx1c2VyLXNjYWxhYmxlPW5vLHZpZXdwb3J0LWZpdD1jb3ZlciI+PG1ldGEgaHR0cC1lcXVpdj0iQ2FjaGUtQ29udHJvbCIgY29udGVudD0ibm8tdHJhbnNmb3JtIj48bWV0YSBodHRwLWVxdWl2PSJDb250ZW50LVR5cGUiIGNvbnRlbnQ9InRleHQvaHRtbDtjaGFyc2V0PXV0Zi04Ij48bWV0YSBuYW1lPSJhcHBsaWNhYmxlLWRldmljZSIgY29udGVudD0ibW9iaWxlIj48bWV0YSBuYW1lPSJmb3JjZS1yZW5kZXJpbmciIGNvbnRlbnQ9IndlYmtpdCI+PG1ldGEgbmFtZT0iYXBwbGUtbW9iaWxlLXdlYi1hcHAtY2FwYWJsZSIgY29udGVudD0ieWVzIj48bWV0YSBuYW1lPSJmdWxsLXNjcmVlbiIgY29udGVudD0idHJ1ZSI+PG1ldGEgbmFtZT0ic2NyZWVuLW9yaWVudGF0aW9uIiBjb250ZW50PSJwb3J0cmFpdCI+PG1ldGEgbmFtZT0ieDUtZnVsbHNjcmVlbiIgY29udGVudD0idHJ1ZSI+PG1ldGEgbmFtZT0iMzYwLWZ1bGxzY3JlZW4iIGNvbnRlbnQ9InRydWUiPjxtZXRhIG5hbWU9ImFwcGxlLW1vYmlsZS13ZWItYXBwLWNhcGFibGUiIGNvbnRlbnQ9InllcyI+PG1ldGEgbmFtZT0iYXBwbGUtbW9iaWxlLXdlYi1hcHAtc3RhdHVzLWJhci1zdHlsZSIgY29udGVudD0iYmxhY2siPjxtZXRhIG5hbWU9ImZvcm1hdC1kZXRlY3Rpb24iIGNvbnRlbnQ9InRlbGVwaG9uZT1ubyI+PG1ldGEgbmFtZT0ic3BtX3ByZWZpeCIgY29udGVudD0iODg4LjEzMDk3Ij4KICAgICAgPHNjcmlwdCB0eXBlPSJ0ZXh0L2phdmFzY3JpcHQiPgogICAgICAgIHdpbmRvdy5hY3Rpdml0eSA9IHtpZDogMjIyNDgsIHNwbUlkOiAnODg4LjEzMDk3J307CiAgICAgICAgd2luZG93LnJlcG9ydE1zZ09iaiA9IHt9OwogICAgICAgIHdpbmRvdy5yZXBvcnRDb25maWcgPSB7CiAgICAgICAgICBzYW1wbGU6IDEsCiAgICAgICAgICBzY3JvbGxUcmFja2VyOiB0cnVlLAogICAgICAgICAgbXNnT2JqZWN0czogJ3JlcG9ydE1zZ09iaicsCiAgICAgICAgICBlcnJvclRyYWNrZXI6IHRydWUKICAgICAgICB9OwogICAgICA8L3NjcmlwdD4KICAgICAgPHNjcmlwdCBzcmM9Ii8vczEuaGRzbGIuY29tL2Jmcy9zZWVkL2xvZy9yZXBvcnQvbG9nLXJlcG9ydGVyLmpzIj48L3NjcmlwdD4KICAgIDxzY3JpcHQgc3JjPSJodHRwczovL3N0YXRpYy5oZHNsYi5jb20vanMvanF1ZXJ5Lm1pbi5qcyI+PC9zY3JpcHQ+PHNjcmlwdCBzcmM9Imh0dHBzOi8vczEuaGRzbGIuY29tL2Jmcy9hY3Rpdml0eS1zZWVkL2FjdGl2aXR5L2FjdGl2aXR5L2FjdGl2aXR5LXJlcG9ydC1wYy5qcyIgY3Jvc3NvcmlnaW49ImFub255bW91cyI+PC9zY3JpcHQ+PHNjcmlwdD52YXIgdWEgPSBuYXZpZ2F0b3IudXNlckFnZW50LnRvTG93ZXJDYXNlKCk7CiAgICB2YXIgaXNJRSA9ICEhd2luZG93LkFjdGl2ZVhPYmplY3QgfHwgJ0FjdGl2ZVhPYmplY3QnIGluIHdpbmRvdzsKICAgIHZhciBpc1NhZmFyaSA9IHVhLmluZGV4T2YoJ3NhZmFyaScpID4gLTEgJiYgdWEuaW5kZXhPZignY2hyb21lJykgPT09IC0xOwogICAgLy8gdmFyIGlzU291cmNlMSA9IGxvY2F0aW9uLmhyZWYuaW5kZXhPZignaHR0cHM6Ly93d3cuYmlsaWJpbGkuY29tJykgPiAtMTsKICAgIGlmIChpc0lFKSB7CiAgICAgIGxvY2F0aW9uLmhyZWYgPSAnaHR0cHM6Ly93d3cuYmlsaWJpbGkuY29tL3ZpZGVvL0JWMXl0NHkxWDc1Qj9mcm9tPScgKyAoaXNTb3VyY2UxID8gJ2RyYWdpbl9zb3VyY2UxJyA6ICdkcmFnaW5fc291cmNlMicpOwogICAgfTwvc2NyaXB0PjxzY3JpcHQ+d2luZG93LmFjdGl2aXR5ID0geyBpZDogIjIyMjQ4IiB9OwogICAgIShmdW5jdGlvbiAoZ2xvYmFsKSB7CiAgICAgIHZhciB1YSA9IG5hdmlnYXRvci51c2VyQWdlbnQ7CiAgICAgIHZhciBpc1N5bWJpYW4gPQogICAgICAgIC8oPzpTeW1iaWFuT1MpLy50ZXN0KHVhKSB8fCAvKD86V2luZG93cyBQaG9uZSkvLnRlc3QodWEpOwogICAgICB2YXIgaXNBbmRyb2lkID0gLyg/OkFuZHJvaWQpLy50ZXN0KHVhKTsKICAgICAgdmFyIGlzVGFibGV0ID0KICAgICAgICAvKD86aVBhZHxQbGF5Qm9vaykvLnRlc3QodWEpIHx8CiAgICAgICAgKGlzQW5kcm9pZCAmJiAhLyg/Ok1vYmlsZSkvLnRlc3QodWEpKSB8fAogICAgICAgICgvKD86RmlyZWZveCkvLnRlc3QodWEpICYmIC8oPzpUYWJsZXQpLy50ZXN0KHVhKSk7CiAgICAgIHZhciBpc1Bob25lID0gLyg/OmlQaG9uZSkvLnRlc3QodWEpOwogICAgICB2YXIgaXNNb2JpbGUgPSAvUGhvbmV8QW5kcm9pZHxpUGhvbmV8aVBhZHxQbGF5Qm9va3xNb2JpbGV8VGFibGV0Ly50ZXN0KHVhKTsKICAgICAgdmFyIGlzUGMgPSAhaXNNb2JpbGU7CiAgICAgIHZhciBqdW1wVXJsID0KICAgICAgICAiaHR0cHM6Ly93d3cuYmlsaWJpbGkuY29tL2g1IiArCiAgICAgICAgbG9jYXRpb24uc2VhcmNoOwogICAgICB2YXIgcGxhdCA9ICJQQyI7CiAgICAgIGlmICgoaXNNb2JpbGUgfHwgaXNUYWJsZXQpICYmIChsb2NhdGlvbi5ocmVmLmluZGV4T2YoJ3ByZXZpZXcnKSA8PSAtMSkpIHsKICAgICAgICBnbG9iYWwubG9jYXRpb24uaHJlZiA9IGp1bXBVcmw7CiAgICAgIH0KICAgIH0pKHdpbmRvdyk7CgogICAgZnVuY3Rpb24gYWRhcHQoZGVzaWduV2lkdGgsIHJlbTJweCkgewogICAgICB2YXIgZCA9IHdpbmRvdy5kb2N1bWVudC5jcmVhdGVFbGVtZW50KCJwIik7CiAgICAgIGQuc3R5bGUud2lkdGggPSAiMXJlbSI7CiAgICAgIGQuc3R5bGUuZGlzcGxheSA9ICJub25lIjsKICAgICAgdmFyIGhlYWQgPSB3aW5kb3cuZG9jdW1lbnQuZ2V0RWxlbWVudHNCeVRhZ05hbWUoImhlYWQiKVswXTsKICAgICAgaGVhZC5hcHBlbmRDaGlsZChkKTsKICAgICAgdmFyIGRlZmF1bHRGb250U2l6ZSA9IHBhcnNlRmxvYXQoCiAgICAgICAgd2luZG93LmdldENvbXB1dGVkU3R5bGUoZCwgbnVsbCkuZ2V0UHJvcGVydHlWYWx1ZSgid2lkdGgiKQogICAgICApOwogICAgICByZXR1cm4gZGVmYXVsdEZvbnRTaXplOwogICAgfQoKICAgICEoZnVuY3Rpb24gKGRvYywgd2luLCBkZXNpZ25XaWR0aCwgcmVtMnB4KSB7CiAgICAgIHZhciBkb2NFbCA9IGRvYy5kb2N1bWVudEVsZW1lbnQsCiAgICAgICAgZGVmYXVsdEZvbnRTaXplID0gYWRhcHQoZGVzaWduV2lkdGgsIHJlbTJweCksCiAgICAgICAgcmVzaXplRXZ0ID0KICAgICAgICAgICJvcmllbnRhdGlvbmNoYW5nZSIgaW4gd2luZG93ID8gIm9yaWVudGF0aW9uY2hhbmdlIiA6ICJyZXNpemUiLAogICAgICAgIHJlY2FsYyA9IGZ1bmN0aW9uICgpIHsKICAgICAgICAgIHZhciBjbGllbnRXaWR0aCA9CiAgICAgICAgICAgIHdpbi5pbm5lcldpZHRoIHx8CiAgICAgICAgICAgIGRvYy5kb2N1bWVudEVsZW1lbnQuY2xpZW50V2lkdGggfHwKICAgICAgICAgICAgZG9jLmJvZHkuY2xpZW50V2lkdGg7CgogICAgICAgICAgaWYgKCFjbGllbnRXaWR0aCkgcmV0dXJuOwogICAgICAgICAgaWYgKGNsaWVudFdpZHRoIDwgMTkyMCkgewogICAgICAgICAgICB3aW5kb3cuYmFzZUZvbnRTaXplID0gKGNsaWVudFdpZHRoIC8gZGVzaWduV2lkdGgpICogcmVtMnB4IC8gMjsKICAgICAgICAgICAgZG9jRWwuc3R5bGUuZm9udFNpemUgPQogICAgICAgICAgICAgICgoKGNsaWVudFdpZHRoIC8gZGVzaWduV2lkdGgpICogcmVtMnB4IC8gMikgLyBkZWZhdWx0Rm9udFNpemUpICoKICAgICAgICAgICAgICAxMDAgKwogICAgICAgICAgICAgICIlIjsKICAgICAgICAgIH0gZWxzZSB7CiAgICAgICAgICAgIGRvY0VsLnN0eWxlLmZvbnRTaXplID0gIjUwcHgiOwogICAgICAgICAgfQogICAgICAgIH07CiAgICAgIGlmICghZG9jLmFkZEV2ZW50TGlzdGVuZXIpIHJldHVybjsKICAgICAgd2luLmFkZEV2ZW50TGlzdGVuZXIocmVzaXplRXZ0LCByZWNhbGMsIGZhbHNlKTsKICAgICAgcmVjYWxjKCk7CiAgICB9KShkb2N1bWVudCwgd2luZG93LCAxOTIwLCAxMDApOzwvc2NyaXB0PjxzY3JpcHQgc3JjPSJodHRwczovL2ludGVyZmFjZS5iaWxpYmlsaS5jb20vc2VydmVyZGF0ZS5qcyI+PC9zY3JpcHQ+PHN0eWxlPiNsb2FkaW5nIHsKICAgICAgcG9zaXRpb246IGZpeGVkOwogICAgICBoZWlnaHQ6IDEwMCU7CiAgICAgIHdpZHRoOiAxMDAlOwogICAgICB0cmFuc2l0aW9uOiBvcGFjaXR5IDUwMG1zIGVhc2U7CiAgICAgIGJhY2tncm91bmQtY29sb3I6ICMwMzAxMjQ7CiAgICAgIGRpc3BsYXk6IGZsZXg7CiAgICAgIGp1c3RpZnktY29udGVudDogY2VudGVyOwogICAgICBhbGlnbi1pdGVtczogY2VudGVyOwogICAgICBjb2xvcjogI2ZmZmZmZjsKICAgICAgei1pbmRleDogOTk5OwogICAgfQoKICAgICNsb2FkaW5nLmxlYXZlIHsKICAgICAgb3BhY2l0eTogMDsKICAgIH0KCiAgICBib2R5IHsKICAgICAgbWFyZ2luOiAwOwogICAgICBmb250LWZhbWlseTogLWFwcGxlLXN5c3RlbSwgQmxpbmtNYWNTeXN0ZW1Gb250LCBIZWx2ZXRpY2EgTmV1ZSwgSGVsdmV0aWNhLAogICAgICAgIEFyaWFsLCBQaW5nRmFuZyBTQywgSGlyYWdpbm8gU2FucyBHQiwgTWljcm9zb2Z0IFlhSGVpLCBzYW5zLXNlcmlmOwogICAgICBmb250LXNpemU6IDEycHg7CiAgICB9CgogICAgYm9keSwKICAgIGh0bWwgewogICAgICB3aWR0aDogMTAwJTsKICAgICAgaGVpZ2h0OiAxMDAlOwogICAgICBvdmVyZmxvdzogaGlkZGVuOwogICAgICBiYWNrZ3JvdW5kLWNvbG9yOiAjMDMwMTI0OwogICAgfQoKICAgICNsb2FkaW5nIC5tYXRlcmlhbHMgLnBpY3R1cmUgewogICAgICBwb3NpdGlvbjogcmVsYXRpdmU7CiAgICAgIHdpZHRoOiAyMzZweDsKICAgICAgaGVpZ2h0OiAyMjBweDsKICAgIH0KCiAgICAjbG9hZGluZyAubWF0ZXJpYWxzIC5wcm9ncmVzcyB7CiAgICAgIHBvc2l0aW9uOiByZWxhdGl2ZTsKICAgICAgYm90dG9tOiAtMTBweDsKICAgICAgdGV4dC1hbGlnbjogY2VudGVyOwogICAgICBjb2xvcjogIzAwRUVGRjsKICAgIH0KCiAgICAjbG9hZGluZyAubWF0ZXJpYWxzIC5waWN0dXJlIC5jbnQgewogICAgICBwb3NpdGlvbjogYWJzb2x1dGU7CiAgICAgIGhlaWdodDogMTAwJTsKICAgICAgd2lkdGg6IDEwMCU7CiAgICAgIGxlZnQ6IDA7CiAgICAgIGJhY2tncm91bmQtcG9zaXRpb246IGNlbnRlcjsKICAgICAgdG9wOiAwOwogICAgICBiYWNrZ3JvdW5kLWltYWdlOiB1cmwoaHR0cHM6Ly9pMC5oZHNsYi5jb20vYmZzL2FjdGl2aXR5LXBsYXQvc3RhdGljLzIwMjAwNjE3LzVmNTdiMzk2ZGQ1ZDRkNjRjZDc4MjFkZTQ1Y2EyNjZlL0FzaDRwQnNpd1QucG5nKTsKICAgICAgei1pbmRleDogMzsKICAgIH0KCiAgICAjbG9hZGluZyAubWF0ZXJpYWxzIC5sYXllcjEgewogICAgICBiYWNrZ3JvdW5kLWNvbG9yOiAjMjA0NzgzOwogICAgICB6LWluZGV4OiAxOwogICAgfQoKICAgICNsb2FkaW5nIC5tYXRlcmlhbHMgLmxheWVyMiB7CiAgICAgIGJhY2tncm91bmQtaW1hZ2U6IGxpbmVhci1ncmFkaWVudCgwZGVnLCAjMDBlZWZmLCAjMTQ5MWZmKTsKICAgICAgei1pbmRleDogMjsKICAgICAgdHJhbnNmb3JtOiB0cmFuc2xhdGVZKDEwMCUpOwogICAgfQoKICAgICNsb2FkaW5nIC5tYXRlcmlhbHMgLmJveCAubGF5ZXIxLAogICAgI2xvYWRpbmcgLm1hdGVyaWFscyAuYm94IC5sYXllcjIgewogICAgICBwb3NpdGlvbjogYWJzb2x1dGU7CiAgICAgIHdpZHRoOiAxMDAlOwogICAgICBoZWlnaHQ6IDEwMCU7CiAgICAgIGNvbnRlbnQ6ICcgJzsKICAgIH0KCiAgICAjbG9hZGluZyAubWF0ZXJpYWxzIC5ib3ggewogICAgICBwb3NpdGlvbjogYWJzb2x1dGU7CiAgICAgIHdpZHRoOiAxNDBweDsKICAgICAgaGVpZ2h0OiAxMTRweDsKICAgICAgY29udGVudDogJyAnOwogICAgICBsZWZ0OiA0N3B4OwogICAgICB0b3A6IDY4cHg7CiAgICAgIG92ZXJmbG93OiBoaWRkZW47CiAgICB9PC9zdHlsZT48bGluayBocmVmPSJodHRwczovL2FjdGl2aXR5Lmhkc2xiLmNvbS9ibGFja2JvYXJkL2FjdGl2aXR5MjIyNDgvbWFpbi5mNmY0MzVmZGY1YzkzODM5ZDczNS5jc3MiIHJlbD0ic3R5bGVzaGVldCI+PC9oZWFkPjxib2R5PjxkaXYgaWQ9ImxvYWRpbmciPjxkaXYgY2xhc3M9Im1hdGVyaWFscyI+PGRpdiBjbGFzcz0icGljdHVyZSI+PGRpdiBjbGFzcz0iY250Ij48L2Rpdj48ZGl2IGNsYXNzPSJib3giPjxkaXYgY2xhc3M9ImxheWVyMSI+PC9kaXY+PGRpdiBjbGFzcz0ibGF5ZXIyIj48L2Rpdj48L2Rpdj48L2Rpdj48ZGl2IGNsYXNzPSJwcm9ncmVzcyI+TG9hZGluZy4uLjxzcGFuIGNsYXNzPSJ0ZXh0Ij48L3NwYW4+PC9kaXY+PC9kaXY+PC9kaXY+PGRpdiBjbGFzcz0iYmctdmlkZW9zIj48L2Rpdj48Y2FudmFzIGlkPSJiYWNrZ3JvdW5kIj48L2NhbnZhcz48ZGl2IGNsYXNzPSJpZSI+PC9kaXY+PGRpdiBjbGFzcz0iYXBwIj48L2Rpdj48c2NyaXB0IHNyYz0iaHR0cHM6Ly9hY3Rpdml0eS5oZHNsYi5jb20vYmxhY2tib2FyZC9hY3Rpdml0eTIyMjQ4L21haW4uN2Y2Mjc3NjM0ZGYwOWU5ZTNkODkuanMiIGNyb3Nzb3JpZ2luPSJhbm9ueW1vdXMiPjwvc2NyaXB0PjwvYm9keT48L2h0bWw+",
                        "headers": [
                            {
                                "key": "Vary",
                                "value": "Origin,Accept-Encoding"
                            },
                            {
                                "key": "X-Cache-Webcdn",
                                "value": "MISS from blzone06"
                            },
                            {
                                "key": "X-Cache-Time",
                                "value": "0"
                            },
                            {
                                "key": "Content-Type",
                                "value": "text/html; charset=utf-8"
                            },
                            {
                                "key": "Set-Cookie",
                                "value": "timeMachine=0; Domain=.bilibili.com; Path=/; HttpOnly; Secure"
                            },
                            {
                                "key": "Expires",
                                "value": "Mon, 24 Nov 2025 08:57:03 GMT"
                            },
                            {
                                "key": "Cache-Control",
                                "value": "no-cache"
                            },
                            {
                                "key": "X-Save-Date",
                                "value": "Mon, 24 Nov 2025 08:57:04 GMT"
                            },
                            {
                                "key": "Date",
                                "value": "Mon, 24 Nov 2025 08:57:04 GMT"
                            },
                            {
                                "key": "Referrer-Policy",
                                "value": "no-referrer-when-downgrade"
                            }
                        ],
                        "reason": "OK",
                        "status": 200
                    },
                    "response_title": "BILIBILI 11周年演讲",
                    "url": "http://bilibili.com/11"
                },
                {
                    "response": {
                        "body": "PCFET0NUWVBFIGh0bWw+PGh0bWw+PGhlYWQ+PG1ldGEgY2hhcnNldD11dGYtOD48bWV0YSBuYW1lPXZpZXdwb3J0IGNvbnRlbnQ9IndpZHRoPWRldmljZS13aWR0aCxpbml0aWFsLXNjYWxlPTEsbWluaW11bS1zY2FsZT0xLG1heGltdW0tc2NhbGU9MSx1c2VyLXNjYWxhYmxlPW5vIj48bWV0YSBuYW1lPXJlbmRlcmVyIGNvbnRlbnQ9d2Via2l0PjxtZXRhIGh0dHAtZXF1aXY9WC1VQS1Db21wYXRpYmxlIGNvbnRlbnQ9IklFPWVkZ2UiPjxtZXRhIG5hbWU9a2V5d29yZHMgY29udGVudD0iQmlsaWJpbGksQUNHLEJpbGliaWxpIGNvcnBvcmF0ZSxNdWx0aSBNZWRpYSBPZmZlcmluZyI+PG1ldGEgbmFtZT1kZXNjcmlwdGlvbiBjb250ZW50PSJCaWxpYmlsaSBhYm91dCB1cyI+PG1ldGEgbmFtZT1zcG1fcHJlZml4IGNvbnRlbnQ9MzMzLjE1OT48dGl0bGU+QWJvdXQtdXM8L3RpdGxlPjxsaW5rIHJlbD1kbnMtcHJlZmV0Y2ggaHJlZj0vL3MxLmhkc2xiLmNvbT48bGluayByZWw9ZG5zLXByZWZldGNoIGhyZWY9Ly9pMC5oZHNsYi5jb20+PGxpbmsgcmVsPWRucy1wcmVmZXRjaCBocmVmPS8vaTEuaGRzbGIuY29tPjxsaW5rIHJlbD1kbnMtcHJlZmV0Y2ggaHJlZj0vL2kyLmhkc2xiLmNvbT48bGluayByZWw9ZG5zLXByZWZldGNoIGhyZWY9Ly9zdGF0aWMuaGRzbGIuY29tPjxsaW5rIHJlbD1wcmVsb2FkIGhyZWY9Ly9zdGF0aWMuaGRzbGIuY29tL2pzL2pxdWVyeS5taW4uanMgYXM9c2NyaXB0PjxsaW5rIHJlbD1wcmVsb2FkIGhyZWY9Ly9zMS5oZHNsYi5jb20vYmZzL3NlZWQvamlua2VsYS9oZWFkZXIvaGVhZGVyLmpzIGFzPXNjcmlwdD48bGluayByZWw9InNob3J0Y3V0IGljb24iIGhyZWY9Ly9zdGF0aWMuaGRzbGIuY29tL2ltYWdlcy9mYXZpY29uLmljbz48bGluayBocmVmPS8vczEuaGRzbGIuY29tL2Jmcy9zdGF0aWMvc3RhdGljL2Nzcy9hcHAuZjE2ODU2ODg4Yzg2OTZjZDZjMDcyNDc2MjgxM2IzZmYuY3NzIHJlbD1zdHlsZXNoZWV0PjwvaGVhZD48Ym9keT48ZGl2IGNsYXNzPXotdG9wLWNvbnRhaW5lcj48L2Rpdj48IS0tIFtpZiBsdCBJRSA5XT4KICAgICAgPGRpdiBjbGFzcz0idXBkYXRlX2Jyb3dzZXIiPkluIG9yZGVyIHRvIHByb3RlY3QgeW91ciBhY2NvdW50IHNlY3VyaXR5PGJyPmJpbGliaWxpIGRvZXMgbm90IHN1cHBvcnQgSUU4IGFuZCBiZWxvdyBicm93c2VyIGFjY2Vzczxicj55b3UgY2FuIHVzZSBDaHJvbWUgYW5kIG90aGVyIG1vZGVybiBicm93c2VycyEmbmJzcDsKICAgICAgICA8YSB0YXJnZXQ9Il9ibGFuayIgY2xhc3M9ImFsZXJ0LWxpbmsiIGhyZWY9Imh0dHA6Ly9icm93c2VoYXBweS5jb20iPlVwZ3JhZGUgbm93ICE8L2E+CiAgICAgIDwvZGl2PgogICAgPCFbZW5kaWZdIC0tPjxkaXYgaWQ9YXBwPjwvZGl2PjxzY3JpcHQgdHlwZT10ZXh0L2phdmFzY3JpcHQgc3JjPS8vc3RhdGljLmhkc2xiLmNvbS9qcy9qcXVlcnkubWluLmpzPjwvc2NyaXB0PjxzY3JpcHQ+LyogZXNsaW50LWRpc2FibGUgKi8KICAgICAgKGZ1bmN0aW9uKCkgewogICAgICAgIHZhciBobSA9IGRvY3VtZW50LmNyZWF0ZUVsZW1lbnQoJ3NjcmlwdCcpOwogICAgICAgIGhtLnNyYyA9ICcvL3MxLmhkc2xiLmNvbS9iZnMvc2VlZC9qaW5rZWxhL2hlYWRlci9oZWFkZXIuanMnOwogICAgICAgIHZhciBzID0gZG9jdW1lbnQuZ2V0RWxlbWVudHNCeVRhZ05hbWUoJ2JvZHknKVswXTsKICAgICAgICBzLnBhcmVudE5vZGUuaW5zZXJ0QmVmb3JlKGhtLCBzKTsKICAgICAgfSkoKTsKCiAgICAgIHdpbmRvdy5yZXBvcnRNc2dPYmogPSB7fTsKICAgICAgd2luZG93LnJlcG9ydENvbmZpZyA9IHsKICAgICAgICBzYW1wbGU6IDEsCiAgICAgICAgc2Nyb2xsVHJhY2tlcjogZmFsc2UsCiAgICAgICAgbXNnT2JqZWN0czogJ3JlcG9ydE1zZ09iaicsCiAgICAgIH07CgogICAgICB2YXIgcmVwb3J0U2NyaXB0ID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnc2NyaXB0Jyk7CiAgICAgIHJlcG9ydFNjcmlwdC5zcmMgPSAnLy9zMS5oZHNsYi5jb20vYmZzL3NlZWQvbG9nL3JlcG9ydC9sb2ctcmVwb3J0ZXIuanMnOwogICAgICBkb2N1bWVudC5nZXRFbGVtZW50c0J5VGFnTmFtZSgnYm9keScpWzBdLmFwcGVuZENoaWxkKHJlcG9ydFNjcmlwdCk7PC9zY3JpcHQ+PCEtLSBDb3B5cmlnaHQgYmlsaWJpbGkKCiAgICBMaWNlbnNlZCB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGUgIkxpY2Vuc2UiKTsKICAgIHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2Ugd2l0aCB0aGUgTGljZW5zZS4KICAgIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdAoKICAgICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjAKCiAgICBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlCiAgICBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiAiQVMgSVMiIEJBU0lTLAogICAgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuCiAgICBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kCiAgICBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS4gLS0+PHNjcmlwdCB0eXBlPXRleHQvamF2YXNjcmlwdCBzcmM9Ly9zMS5oZHNsYi5jb20vYmZzL3N0YXRpYy9zdGF0aWMvanMvbWFuaWZlc3QuNGZmYjMxODg3MjI5ZTAwMjVhNTYuanM+PC9zY3JpcHQ+PHNjcmlwdCB0eXBlPXRleHQvamF2YXNjcmlwdCBzcmM9Ly9zMS5oZHNsYi5jb20vYmZzL3N0YXRpYy9zdGF0aWMvanMvdmVuZG9yLmZmNWJkNTZjZGM3ODA1OTc1OGYzLmpzPjwvc2NyaXB0PjxzY3JpcHQgdHlwZT10ZXh0L2phdmFzY3JpcHQgc3JjPS8vczEuaGRzbGIuY29tL2Jmcy9zdGF0aWMvc3RhdGljL2pzL2FwcC5iNzFmNDE5NjM2OWE2MTVlZTdmYS5qcz48L3NjcmlwdD48L2JvZHk+PC9odG1sPg==",
                        "headers": [
                            {
                                "key": "Etag",
                                "value": "W/\"5d1b879c-c30\""
                            },
                            {
                                "key": "Vary",
                                "value": "Origin,Accept-Encoding"
                            },
                            {
                                "key": "Cache-Control",
                                "value": "no-cache"
                            },
                            {
                                "key": "Server",
                                "value": "openresty"
                            },
                            {
                                "key": "Expires",
                                "value": "Mon, 24 Nov 2025 08:57:20 GMT"
                            },
                            {
                                "key": "X-Cache-Webcdn",
                                "value": "MISS from blzone06"
                            },
                            {
                                "key": "X-Cache-Time",
                                "value": "0"
                            },
                            {
                                "key": "X-Save-Date",
                                "value": "Mon, 24 Nov 2025 08:57:21 GMT"
                            },
                            {
                                "key": "Date",
                                "value": "Mon, 24 Nov 2025 08:57:21 GMT"
                            },
                            {
                                "key": "Content-Type",
                                "value": "text/html; charset=UTF-8"
                            },
                            {
                                "key": "Last-Modified",
                                "value": "Tue, 02 Jul 2019 16:34:36 GMT"
                            }
                        ],
                        "reason": "OK",
                        "status": 200
                    },
                    "response_title": "About-us",
                    "url": "http://bilibili.com/aboutus"
                }
            ]
        }`
	//bData, _ := json.Marshal(map[string]interface{}{"query_type": "baidu.com", "domain": "baidu.com"})
	apb, err := j.JSONToAnyPB(context.Background(), "DirBruteTaskResults", []byte(bd))
	if err != nil {
		t.Error(err)
	}
	pbd, err := j.AnyPBToJSON(context.Background(), "DirBruteTaskResults", apb)
	if err != nil {
		t.Error(err)
	}
	println(string(pbd))
}

//func TestConvJsonToPB(t *testing.T) {
//	var a = map[string]interface{}{
//		"ip":     "123",
//		"domain": "1111",
//		"rate":   233,
//	}
//	taskData, _ := sonic.Marshal(a)
//	aa, err := JSONToPB(context.Background(), "https://github.acme.red/mapper/idl/blob/main/proto/mapper/taskargs/v1/args.proto", "ServiceProbeTaskData", taskData)
//	if err != nil {
//		t.Fatal(err)
//	}
//	println(aa.String())
//}

//func TestConvertToRawURL(t *testing.T) {
//	u, err := url.Parse("https://github.acme.red/mapper/idl/blob/main/proto/mapper/taskargs/v1/args.proto")
//	if err != nil {
//		panic(err)
//	}
//	println(u.Scheme, u.Host, u.RequestURI())
//	uPath := strings.Split(u.Path, "/")
//	var pb = githubContent{}
//	var gPath = make([]string, 0)
//	for _, pth := range uPath {
//		if pth == "" || pth == "blob" {
//			continue
//		}
//		if pb.Owner == "" {
//			pb.Owner = pth
//			continue
//		}
//		if pb.Repo == "" {
//			pb.Repo = pth
//			continue
//		}
//		if pb.Branch == "" {
//			pb.Branch = pth
//			continue
//		}
//		gPath = append(gPath, pth)
//	}
//	pb.Path = strings.Join(gPath, "/")
//	println(sonic.MarshalString(pb))
//}
