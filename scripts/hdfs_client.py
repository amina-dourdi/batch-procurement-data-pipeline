import os
import requests
from urllib.parse import quote

#WebHDFS REST API
class WebHDFSClient:
    def __init__(self, base_url: str, user: str = "root"):
        self.base_url = base_url.rstrip("/")
        self.user = user
    #Internal URL builder
    def _url(self, hdfs_path: str, op: str, extra: str = "") -> str:
        safe_path = "/".join(quote(p) for p in hdfs_path.strip("/").split("/"))
        url = f"{self.base_url}/webhdfs/v1/{safe_path}?op={op}&user.name={quote(self.user)}"
        if extra:
            url += "&" + extra
        return url
    #hdfs dfs -mkdir -p /raw/orders/2026-01-14
    def mkdirs(self, hdfs_dir: str) -> None:
        r = requests.put(self._url(hdfs_dir, "MKDIRS"), timeout=60)
        r.raise_for_status()
 
    #Check if a file or folder exists:hdfs dfs -test -e /raw/orders
    def exists(self, hdfs_path: str) -> bool:
        r = requests.get(self._url(hdfs_path, "GETFILESTATUS"), timeout=60)
        if r.status_code == 404:
            return False
        r.raise_for_status()
        return True
    #Upload a file to HDFS :
    #In distributed systems → fewer calls = safer & faster.
    # ❌ 2. Extra network call
        # You make:
            # 1 HTTP call to exists
            # 1 HTTP call to CREATE

        # Instead of just:
        # 1 HTTP call to CREATE

    def put_file(self, local_path: str, hdfs_path: str, overwrite: bool = False) -> None: #Let HDFS handle it.
        extra = f"overwrite={'true' if overwrite else 'false'}"
        # File exists & overwrite=true	File is replaced ✅
        # File exists & overwrite=false	HDFS rejects ❌

        r1 = requests.put(self._url(hdfs_path, "CREATE", extra=extra), allow_redirects=False, timeout=60)
        if r1.status_code not in (307, 201):
            r1.raise_for_status()
        redirect = r1.headers.get("Location")
        if not redirect:
            return
        #hdfs dfs -put local.txt /raw/data/local.txt
        with open(local_path, "rb") as f:
            r2 = requests.put(redirect, data=f, timeout=300)
        r2.raise_for_status()
    #Download a file from HDFS : hdfs dfs -get /raw/data/file.txt ./file.txt -> That command also prints nothing, but the file appears locally.
    def get_file(self, hdfs_path: str, local_path: str) -> None:
        r = requests.get(self._url(hdfs_path, "OPEN"), allow_redirects=True, stream=True, timeout=60)
        r.raise_for_status()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    def delete(self, path, recursive=False):
        
        extra = f"recursive={'true' if recursive else 'false'}"
        url = self._url(path, "DELETE", extra=extra)
        resp = requests.delete(url)
        return resp.status_code == 200