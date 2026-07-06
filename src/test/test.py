import os

java_path = os.path.join(
    os.environ["JAVA_HOME"],
    "bin",
    "java.exe"
)

print(java_path)
print(os.path.exists(java_path))