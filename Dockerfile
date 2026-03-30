FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /src

COPY ksn-disconnect.csproj ./
RUN dotnet restore

COPY . ./

RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) rid_arch="x64" ;; \
      arm64) rid_arch="arm64" ;; \
      *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    dotnet publish ksn-disconnect.csproj \
      -c Release \
      -r "${TARGETOS}-${rid_arch}" \
      --self-contained true \
      -p:PublishTrimmed=false \
      -o /out

FROM mcr.microsoft.com/dotnet/runtime-deps:8.0-bookworm-slim AS runtime

WORKDIR /app

COPY --from=build /out/ /app/

ENTRYPOINT ["/app/ksn-disconnect"]
