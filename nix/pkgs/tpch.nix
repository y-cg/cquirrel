{
  lib,
  stdenv,
  fetchFromGitHub,
  makeWrapper,
  clang,
}:

stdenv.mkDerivation {
  pname = "tpch-dbgen";
  version = "2.17.3";

  src = fetchFromGitHub {
    owner = "gregrahn";
    repo = "tpch-kit";
    rev = "852ad0a5ee31ebefeed884cea4188781dd9613a3";
    sha256 = "sha256-VNR7FgJa0kc5tIqKvuHHOHhKqukBn1ONHeSjUNKqYaA=";
  };

  nativeBuildInputs = [ makeWrapper ];
  buildInputs = [ clang ];

  buildPhase = ''
    cd dbgen
    make MACHINE=${
      if stdenv.hostPlatform.isDarwin then "MACOS" else "LINUX"
    } DATABASE=POSTGRESQL CC=clang
  '';

  installPhase = ''
    mkdir -p $out/bin
    mkdir -p $out/share/tpch-dbgen

    # Install binaries
    cp dbgen $out/bin/
    cp qgen $out/bin/

    # Install queries and templates
    cp -r queries $out/share/tpch-dbgen/
    cp *.tbl $out/share/tpch-dbgen/ 2>/dev/null || true
    cp *.dss $out/share/tpch-dbgen/ 2>/dev/null || true

    # Wrap binaries with required environment variables
    wrapProgram $out/bin/dbgen \
      --set DSS_CONFIG $out/share/tpch-dbgen \
      --set DSS_QUERY $out/share/tpch-dbgen/queries
    wrapProgram $out/bin/qgen \
      --set DSS_CONFIG $out/share/tpch-dbgen \
      --set DSS_QUERY $out/share/tpch-dbgen/queries
  '';

  meta = with lib; {
    description = "TPC-H benchmark database generator";
    homepage = "http://www.tpc.org/tpch/";
    license = licenses.unfree; # TPC-H license is restrictive
    platforms = platforms.linux ++ platforms.darwin;
    maintainers = [ ];
  };
}
