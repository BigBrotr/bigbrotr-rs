# Isolated trusted research runner, not a BigBrotr distribution image.
# The launcher must verify this local tag resolves to image config
# sha256:7e946a0c0aa4a5d357c3ae517fe9830f675cfc98fd26a9cba79b2bc957928b9d
# before and after building. It is not a published registry manifest digest.
FROM bigbrotr-implementation-ci-6314639:lab
LABEL org.bigbrotr.purpose="isolated-catalog-fence-research"
# Match the already inventoried libpq15.19 in the native build image. The actual
# resulting image, package version and executable hashes are required evidence.
RUN apt-get update \
    && apt-get install --no-install-recommends -y postgresql-client-15=15.19-0+deb12u1 \
    && rm -rf /var/lib/apt/lists/*
