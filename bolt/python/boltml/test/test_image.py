# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import http.server
import io
import socketserver
import threading
import unittest
from unittest.mock import patch

from PIL import Image

import pybolt

from boltml import DataFrame
from boltml.function import vectorFunction
from boltml.function.image import downloadImages, register as registerImageFunctions

from .utils import WithDataFrame, Runtime

executors = Runtime.executors(Runtime.all())
registerImageFunctions(executors)


png_bytes = (
    b"\x89PNG\r\n\x1a\n"
    b"\x00\x00\x00\rIHDR"
    b"\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x02\x00\x00\x00\x90wS\xde"
    b"\x00\x00\x00\nIDAT"
    b"\x08\xd7c\xf8\xcf\xc0\x00\x00"
    b"\x03\x01\x01\x00\x18\xdd\x8dr"
    b"\x00\x00\x00\x00IEND\xaeB`\x82"
)


class TestImageEmptyVectors(unittest.TestCase):
    def test_download_images_preserves_binary_type_without_urls(self):
        for urls in ([], [None, None]):
            with self.subTest(urls=urls):
                with patch("urllib.request.urlopen") as urlopen:
                    result = downloadImages(pybolt.fromList(urls, pybolt.VarcharType()))
                    urlopen.assert_not_called()
                self.assertEqual(result.dtype(), pybolt.VarbinaryType())
                self.assertEqual(list(result), urls)

                frame = DataFrame({"image": result})
                self.assertEqual(len(frame), len(urls))
                self.assertEqual(frame["image"].dtype, pybolt.VarbinaryType())
                self.assertEqual(frame["image"].data.dtype(), pybolt.VarbinaryType())
                self.assertEqual(list(frame["image"]), urls)


class TestBoltImageUDF(unittest.TestCase):
    def setUp(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            png = png_bytes

            def do_GET(self):
                if self.path == "/img.png":
                    self.send_response(200)
                    self.send_header("Content-Type", "image/png")
                    self.send_header("Content-Length", str(len(self.png)))
                    self.end_headers()
                    self.wfile.write(self.png)
                    return
                if self.path == "/text":
                    body = b"hello"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if self.path == "/no-content-type":
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(self.png)))
                    self.end_headers()
                    self.wfile.write(self.png)
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format, *args):
                return

        self._httpd = socketserver.TCPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        thread.start()
        self._server_thread = thread
        host, port = self._httpd.server_address
        self._base_url = f"http://{host}:{port}"

    def tearDown(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._server_thread.join(timeout=5)

    def test_image_pil(self):
        @vectorFunction(pybolt.VarbinaryType(), executors)
        def image_resize_pil(images, width=1, height=1, fmt=None) -> pybolt.BaseVector:
            out = []
            for i in range(len(images)):
                b = images[i]
                if b is None:
                    out.append(None)
                    continue
                img = Image.open(io.BytesIO(b))
                src_fmt = img.format
                img.load()
                img = img.resize((width, height), resample=Image.Resampling.BILINEAR)
                buf = io.BytesIO()
                save_fmt = fmt if fmt is not None else (src_fmt or "PNG")
                img.save(buf, format=save_fmt)
                out.append(buf.getvalue())
            return pybolt.fromList(out, pybolt.VarbinaryType())

        @WithDataFrame.factory(
            lambda plan_builder, executor: DataFrame(
                {"c0": pybolt.fromList([png_bytes] * 5, pybolt.VarbinaryType())},
                executor=executor,
                planFactory=plan_builder,
            )
        )
        def run(self, df):
            df["result"] = df["c0"].map(image_resize_pil, 2, 2)
            self.assertEqual(len(df), 5)
            self.assertEqual(df["result"].dtype, pybolt.VarbinaryType())
            self.assertEqual(df["result"].data.dtype(), pybolt.VarbinaryType())
            for i in range(len(df)):
                b = df[i, "result"]
                self.assertIsInstance(b, (bytes, bytearray))
                self.assertTrue(b.startswith(b"\x89PNG\r\n\x1a\n"))
                img = Image.open(io.BytesIO(b))
                self.assertEqual(img.format, "PNG")
                img.load()
                self.assertEqual(img.size, (2, 2))

        run(self)

    def test_download_images(self):
        @WithDataFrame.factory(
            lambda plan_builder, executor: DataFrame(
                {
                    "url": pybolt.fromList(
                        [
                            f"{self._base_url}/img.png",
                            f"{self._base_url}/text",
                            f"{self._base_url}/missing",
                            f"{self._base_url}/no-content-type",
                            None,
                        ],
                        pybolt.VarcharType(),
                    )
                },
                executor=executor,
                planFactory=plan_builder,
            )
        )
        def run(self, df):
            df["result"] = df["url"].map(downloadImages)
            self.assertEqual(len(df), 5)
            self.assertEqual(df["result"].dtype, pybolt.VarbinaryType())
            self.assertEqual(df["result"].data.dtype(), pybolt.VarbinaryType())
            self.assertEqual(df[0, "result"], png_bytes)
            self.assertEqual(df[1, "result"], None)
            self.assertEqual(df[2, "result"], None)
            self.assertEqual(df[3, "result"], png_bytes)
            self.assertEqual(df[4, "result"], None)

        run(self)
