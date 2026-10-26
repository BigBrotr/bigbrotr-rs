# Private Linux amd64 candidate; consume the verified extracted native bundle.
# Runtime dependencies are pinned by image digest; no compiler or package install.
FROM gcr.io/distroless/cc-debian13@sha256:732d2fe38bb1996cb47f8218b48dc840967684d73c5483894ff2ed638068c1b6
WORKDIR /opt/bigbrotr
COPY bigbrotr build-info.json members.json LICENSE ./
COPY archive-worker/ ./archive-worker/
COPY packaging/archive-worker/notices/ ./notices/
USER 65532:65532
ENTRYPOINT ["/opt/bigbrotr/bigbrotr"]
CMD ["build-info"]
