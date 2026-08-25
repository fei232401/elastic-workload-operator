# mockrouter 只对外监听,无出站请求 → scratch 镜像即可(极小,~2MB)
FROM scratch
COPY mockrouter /mockrouter
ENTRYPOINT ["/mockrouter"]
