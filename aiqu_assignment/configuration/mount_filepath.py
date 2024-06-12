# Databricks notebook source
storageAccountName = "aiqustorage1"
storageAccountAccessKey = "bye+xuoAAPnw3ilXXR0bFVMjO0kopmlS2kwjDYwRXYSvh7ePykxwc1mHi0QtBXW33wmh9sRQJYio+AStvuIwWg=="
blobContainerName = "aiqucontainer"
if not any(mount.mountPoint == '/mnt/analytics/' for mount in dbutils.fs.mounts()):
    try:
        dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
        mount_point = "/mnt/analytics",
        extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
  )
    except Exception as e:
        print(e)
        print("already mounted. Try to unmount first")