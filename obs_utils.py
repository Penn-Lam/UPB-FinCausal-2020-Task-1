import os
import requests
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkcore.http.http_config import HttpConfig
from huaweicloudsdkobs.v1.region.obs_region import OBSRegion
from huaweicloudsdkobs.v1 import *

class OBSUtils:
    def __init__(self, ak, sk, region='cn-north-4'):
        """
        初始化OBS客户端
        :param ak: Access Key ID
        :param sk: Secret Access Key
        :param region: 区域，默认为cn-north-4
        """
        self.credentials = BasicCredentials(ak, sk)
        self.config = HttpConfig.get_default_config()
        self.client = OBSClient.new_builder() \
            .with_credentials(self.credentials) \
            .with_region(OBSRegion.value_of(region)) \
            .with_http_config(self.config) \
            .build()

    def download_from_obs(self, bucket_name, obs_path, local_path):
        """
        从OBS下载文件或目录到本地
        :param bucket_name: OBS桶名
        :param obs_path: OBS上的文件或目录路径
        :param local_path: 本地保存路径
        """
        try:
            # 列出对象
            request = ListObjectsRequest()
            request.bucket = bucket_name
            request.prefix = obs_path
            response = self.client.list_objects(request)

            if response.status_code == 200:
                for obj in response.body.contents:
                    # 获取对象键（完整路径）
                    object_key = obj.key
                    
                    # 构建本地文件路径
                    relative_path = object_key[len(obs_path):].lstrip('/')
                    local_file_path = os.path.join(local_path, relative_path)
                    
                    # 创建本地目录
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    
                    # 获取对象
                    get_request = GetObjectRequest()
                    get_request.bucket = bucket_name
                    get_request.key = object_key
                    get_response = self.client.get_object(get_request)
                    
                    if get_response.status_code == 200:
                        print(f"Downloading {object_key} to {local_file_path}")
                        # 保存文件
                        with open(local_file_path, 'wb') as f:
                            for chunk in get_response.body.read():
                                f.write(chunk)
                    else:
                        print(f"Failed to download {object_key}: {get_response.status_code}")
                
                print("Download completed successfully!")
            else:
                print(f"Failed to list objects: {response.status_code}")
                
        except Exception as e:
            print(f"Error occurred during download: {str(e)}")

    def upload_to_obs(self, bucket_name, local_path, obs_path):
        """
        上传本地文件或目录到OBS
        :param bucket_name: OBS桶名
        :param local_path: 本地文件或目录路径
        :param obs_path: OBS上的目标路径
        """
        try:
            if os.path.isfile(local_path):
                # 上传单个文件
                object_key = os.path.join(obs_path, os.path.basename(local_path))
                self._upload_file(bucket_name, local_path, object_key)
                
            elif os.path.isdir(local_path):
                # 上传目录
                for root, _, files in os.walk(local_path):
                    for file in files:
                        local_file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(local_file_path, local_path)
                        object_key = os.path.join(obs_path, relative_path)
                        
                        self._upload_file(bucket_name, local_file_path, object_key)
                            
                print("Upload completed successfully!")
            else:
                print(f"Path not found: {local_path}")
                
        except Exception as e:
            print(f"Error occurred during upload: {str(e)}")

    def _upload_file(self, bucket_name, local_file_path, object_key):
        """
        上传单个文件到OBS
        """
        print(f"Uploading {local_file_path} to obs://{bucket_name}/{object_key}")
        
        try:
            # 创建上传请求
            request = PutObjectRequest()
            request.bucket = bucket_name
            request.key = object_key
            
            # 读取文件内容
            with open(local_file_path, 'rb') as f:
                request.body = f
                
                # 上传文件
                response = self.client.put_object(request)
                
                if response.status_code < 300:
                    print(f"Upload successful: {object_key}")
                else:
                    print(f"Failed to upload {object_key}: {response.status_code}")
                    
        except Exception as e:
            print(f"Error uploading {local_file_path}: {str(e)}")

# 使用示例
if __name__ == "__main__":
    # 配置信息
    access_key = "8YUN2DXIF9UDBQO4QTFW"
    secret_key = "fN5eibOyxr8BWNXw6pzcjz3drAe23e8FP4YcgfUI"
    endpoint = "https://upb.obs.cn-south-1.myhuaweicloud.com"  # 根据实际区域修改
    bucket_name = "upb"
    
    # 初始化OBS工具类
    obs_utils = OBSUtils(access_key, secret_key, region)
    
    # 下载示例
    obs_utils.download_from_obs(
        bucket_name=bucket_name,
            obs_path="ceshi",  # OBS上的路径
            local_path="./ceshi"  # 本地保存路径
    )
    
    # 上传示例
    obs_utils.upload_to_obs(
        bucket_name=bucket_name,
            local_path="./ceshi",  # 本地文件或目录路径
            obs_path="ceshi/version1"    # OBS上的目标路径
    )
