{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

let
  tpch-dbgen = pkgs.callPackage ./nix/pkgs/tpch.nix { };
in
{
  env.TPCH_DATA_DIR = "${config.devenv.root}/tpch_data";
  env.DUCKDB_RC = "${config.devenv.root}/.duckdbrc";
  env.CQUIRREL_METRICS_OUT = "${config.devenv.root}/result/metrics.jsonl";

  # https://devenv.sh/packages/
  packages = with pkgs; [
    git
    tpch-dbgen
    just
  ];

  scripts = {
    duckdb.exec = ''
      ${lib.getExe pkgs.duckdb} -init "$DUCKDB_RC" "$@"
    '';
    dbgen.exec = ''
      mkdir -p "$TPCH_DATA_DIR"
      cd "$TPCH_DATA_DIR"
      ${tpch-dbgen}/bin/dbgen "$@"
    '';
  };

  # https://devenv.sh/languages/
  languages = {
    java = {
      enable = true;
      jdk.package = pkgs.jdk21;
      maven.enable = true;
      lsp = {
        enable = true;
      };
    };
    nix = {
      enable = true;
    };
  };

  apple.sdk = null;

  # https://devenv.sh/basics/
  enterShell = ''
    dbgen -h 2>&1 | grep --color=auto "${tpch-dbgen.version}"
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    (dbgen -h || true) 2>&1 | grep --color=auto "${tpch-dbgen.version}"
  '';

  # https://devenv.sh/git-hooks/
  git-hooks = {
    hooks = {
      nixfmt.enable = true;
      # tidy up Maven POM files before committing
      mvn-tidy = {
        enable = true;
        name = "Tidy Maven POM files";
        entry = "mvn tidy:pom";
        files = "pom\\.xml$";
        pass_filenames = false;
      };
    };
  };

  # See full reference at https://devenv.sh/reference/options/
}
