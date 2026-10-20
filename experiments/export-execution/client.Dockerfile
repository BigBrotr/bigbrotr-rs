# Research client only. The outer build must verify this retained local base:
# sha256:beb1b1d81ab841af53b71bf545d4184f37ce3e20fd7505eaf1f7f16a74469dd2.
FROM bigbrotr-implementation-foundation:20260917

# Resolve from authenticated Debian metadata; the immutable output image and
# installed package versions are retained with the run. No product image changes.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes postgresql-client-15 \
    && dpkg-query -W > /usr/local/share/export-client-packages.txt \
    && /usr/lib/postgresql/15/bin/psql --version \
    && rm -rf /var/lib/apt/lists/*
