import os
import requests
from urllib.parse import quote

class WebHDFSClient:
    def __init__(self, base_url: str, user: str = "root"):
        self.base_url = base_url.rstrip("/")
        self.user = user

    def _url(self, hdfs_path: str, op: str, extra: str = "") -> str:
        safe_path = "/".join(quote(p) for p in hdfs_path.strip("/").split("/"))
        url = f"{self.base_url}/webhdfs/v1/{safe_path}?op={op}&user.name={quote(self.user)}" #The format is always: http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=<OPERATION>.
        if extra:
            url += "&" + extra
        return url

    # Creates directories in HDFS
    def mkdirs(self, hdfs_dir: str) -> None:
        r = requests.put(self._url(hdfs_dir, "MKDIRS"), timeout=60)
        r.raise_for_status()

    #Checks metadata only (no data read)
    def exists(self, hdfs_path: str) -> bool:
        r = requests.get(self._url(hdfs_path, "GETFILESTATUS"), timeout=60)
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return True

    def put_file(self, local_path: str, hdfs_path: str, overwrite: bool = False) -> None:
        extra = f"overwrite={'true' if overwrite else 'false'}"
        r1 = requests.put(self._url(hdfs_path, "CREATE", extra=extra), allow_redirects=False, timeout=60)
        if r1.status_code not in (307, 201):
            r1.raise_for_status()
        redirect = r1.headers.get("Location")
        if not redirect:
            return
        with open(local_path, "rb") as f:
            r2 = requests.put(redirect, data=f, timeout=300)
        r2.raise_for_status()

    def get_file(self, hdfs_path: str, local_path: str) -> None:
        r = requests.get(self._url(hdfs_path, "OPEN"), allow_redirects=True, stream=True, timeout=60)
        r.raise_for_status()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)