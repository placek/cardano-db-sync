{
  description = "cardano-db-sync";

  inputs = {
    nixpkgs.follows = "haskellNix/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
    haskellNix = {
      url = "github:input-output-hk/haskell.nix";
      inputs.hackage.follows = "hackageNix";
    };
    hackageNix = {
      url = "github:input-output-hk/hackage.nix";
      flake = false;
    };
    iohkNix = {
      url = "github:input-output-hk/iohk-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-compat = {
      url = "github:input-output-hk/flake-compat/fixes";
      flake = false;
    };
    CHaP = {
      url = "github:IntersectMBO/cardano-haskell-packages?ref=repo";
      flake = false;
    };
  };

  outputs = { self, ... }@inputs:
    let
      supportedSystems = [
        "x86_64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
    in
      inputs.utils.lib.eachSystem supportedSystems (system:
        let
          nixpkgs = import inputs.nixpkgs {
            inherit system;
            inherit (inputs.haskellNix) config;

            overlays =
              builtins.attrValues inputs.iohkNix.overlays ++
              [ inputs.haskellNix.overlay

                (final: prev: with final; {
                  # Required in order to build postgresql for musl
                  postgresql = (final.postgresql_11
                    .overrideAttrs (_: lib.optionalAttrs (stdenv.hostPlatform.isMusl) {
                      dontDisableStatic = true;
                      NIX_LDFLAGS = "--push-state --as-needed -lstdc++ --pop-state";
                      # without this collate.icu.utf8, and foreign_data will fail.
                      LC_CTYPE = "C";
                    }))
                    .override {
                      enableSystemd = stdenv.hostPlatform.isLinux && !stdenv.hostPlatform.isMusl;
                      gssSupport = stdenv.hostPlatform.isLinux && !stdenv.hostPlatform.isMusl;
                    };
                })

                (final: prev: {
                  inherit (project.hsPkgs.cardano-node.components.exes) cardano-node;
                  inherit (project.hsPkgs.cardano-cli.components.exes) cardano-cli;
                })

                (final: prev:
                  let
                    profiled = project.profiled.exes;
                  in {
                    # The cardano-db-sync NixOS module (nix/nixos/cardano-db-sync-service.nix)
                    # expects these to be here
                    inherit (project.exes)
                      cardano-db-sync
                      cardano-db-tool
                      cardano-smash-server;

                    cardano-db-sync-profiled = profiled.cardano-db-sync;
                    cardano-smash-server-profiled = profiled.cardano-smash-server;

                    schema = ./schema;
                  })

                (final: prev: {
                  # HLint 3.2.x requires GHC >= 8.10 && < 9.0
                  hlint = final.haskell-nix.tool "ghc8107" "hlint" {
                    version = "3.2.7";
                  };

                  # Fourmolu 0.10.x requires GHC >= 9.0 && < 9.6
                  fourmolu = final.haskell-nix.tool "ghc928" "fourmolu" {
                    version = "0.10.1.0";
                  };

                  # Weeder 2.2.0 requires GHC >= 8.10 && < 9.0
                  weeder = final.haskell-nix.tool "ghc8107" "weeder" {
                    version = "2.2.0";
                  };
                })
              ];
          };

          # Set up and start Postgres before running database tests
          preCheck = ''
            echo pre-check
            initdb --encoding=UTF8 --locale=en_US.UTF-8 --username=postgres $NIX_BUILD_TOP/db-dir
            postgres -D $NIX_BUILD_TOP/db-dir -c listen_addresses="" -k $TMP &
            PSQL_PID=$!
            echo $PSQL_PID > postgres.pid
            sleep 10
            if (echo '\q' | psql -h $TMP postgres postgres); then
              echo "PostgreSQL server is verified to be started."
            else
              echo "Failed to connect to local PostgreSQL server."
              exit 2
            fi
            ls -ltrh $NIX_BUILD_TOP
            DBUSER=$(whoami)
            DBNAME=$DBUSER
            export PGPASSFILE=$NIX_BUILD_TOP/pgpass
            echo "$TMP:5432:$DBUSER:$DBUSER:*" > $PGPASSFILE
            cp -vir ${./schema} ../schema
            chmod 600 $PGPASSFILE
            psql -h $TMP postgres postgres <<EOF
              create role $DBUSER with createdb login password '$DBPASS';
              alter user $DBUSER with superuser;
              create database $DBNAME with owner = $DBUSER;
              \\connect $DBNAME
              ALTER SCHEMA public   OWNER TO $DBUSER;
            EOF
          '';

          # Clean up Postgres after running database tests
          postCheck = ''
            echo post-check
            DBNAME=$(whoami)
            NAME=db_schema.sql
            mkdir -p $out/nix-support
            echo "Dumping schema to db_schema.sql"
            pg_dump -h $TMP -s $DBNAME > $out/$NAME
            kill $(cat postgres.pid)
            echo "Adding to build products..."
            echo "file binary-dist $out/$NAME" > $out/nix-support/hydra-build-products
          '';

          project = (nixpkgs.haskell-nix.cabalProject' ({ config, lib, pkgs, ... }: rec {
            src = ./.;
            name = "cardano-db-sync";
            compiler-nix-name =
              if system == "x86_64-linux"
                then lib.mkDefault "ghc810"
                else lib.mkDefault "ghc96";
            flake.variants =
              let
                compilers =
                  if (system == "x86_64-linux") then
                    ["ghc96" "ghc98"]
                  else
                    ["ghc98"];
              in
                lib.genAttrs compilers (c: { compiler-nix-name = c; });

            crossPlatforms = p:
              lib.optional (system == "x86_64-linux") p.musl64 ++
              lib.optional
                (system == "x86_64-linux" && config.compiler-nix-name == "ghc966")
                p.aarch64-multiplatform-musl;

            inputMap = {
              "https://chap.intersectmbo.org/" = inputs.CHaP;
            };

            shell.tools = {
              cabal = "3.10.3.0";
              ghcid = "0.8.8";
              haskell-language-server = {
                src =
                  if config.compiler-nix-name == "ghc8107" then
                    nixpkgs.haskell-nix.sources."hls-1.10"
                  else
                    nixpkgs.haskell-nix.sources."hls-2.9";
              };
            };
            # Now we use pkgsBuildBuild, to make sure that even in the cross
            # compilation setting, we don't run into issues where we pick tools
            # for the target.
            shell.buildInputs = with nixpkgs.pkgsBuildBuild; [
              gitAndTools.git
              hlint
            ] ++ lib.optionals (config.compiler-nix-name == "ghc8107") [
              # Weeder requires the GHC version to match HIE files
              weeder
            ] ++ lib.optionals (system != "aarch64-darwin") [
              # TODO: Fourmolu 0.10 is currently failing to build with aarch64-darwin
              #
              # Linking dist/build/fourmolu/fourmolu ...
              # ld: line 269:  2352 Segmentation fault ...
              # clang-11: error: linker command failed with exit code 139 (use -v to see invocation)
              # `cc' failed in phase `Linker'. (Exit code: 139)
              fourmolu
            ];
            shell.withHoogle = true;
            shell.crossPlatforms = _: [];

            modules = [
              ({ lib, pkgs, ... }: {
                # Ignore version bounds
                packages.katip.doExactConfig = true;
                # Split data to reduce closure size
                packages.ekg.components.library.enableSeparateDataOutput = true;
              })

              ({
                # Override extra-source-files
                packages.cardano-db-sync.package.extraSrcFiles =
                  [ "../schema/*.sql" ];
                packages.cardano-db.package.extraSrcFiles =
                  ["../config/pgpass-testnet"];
                packages.cardano-db.components.tests.schema-rollback.extraSrcFiles =
                  [ "src/Cardano/Db/Schema.hs" "src/Cardano/Db/Delete.hs" ];
                packages.cardano-db.components.tests.test-db.extraSrcFiles =
                  [ "../config/pgpass-mainnet" ];
                packages.cardano-chain-gen.package.extraSrcFiles =
                  [ "../schema/*.sql" ];

              })

              ({ lib, config, ... }:
                # Disable haddock on 8.x
                lib.mkIf (lib.versionOlder config.compiler.version "9") {
                  packages.cardano-ledger-alonzo.doHaddock = false;
                  packages.cardano-ledger-allegra.doHaddock = false;
                  packages.cardano-ledger-api.doHaddock = false;
                  packages.cardano-ledger-babbage.doHaddock = false;
                  packages.cardano-ledger-conway.doHaddock = false;
                  packages.cardano-ledger-shelley.doHaddock = false;
                  packages.cardano-protocol-tpraos.doHaddock = false;
                  packages.ouroboros-network-framework.doHaddock = false;
                  packages.ouroboros-consensus-cardano.doHaddock = false;
                  packages.ouroboros-consensus.doHaddock = false;
                  packages.cardano-ledger-core.doHaddock = false;
                  packages.wai-extra.doHaddock = false;
                })

              ({ lib, pkgs, config, ... }:
                lib.mkIf (lib.versionAtLeast config.compiler.version "9.4") {
                  # lib:ghc is a bit annoying in that it comes with it's own build-type:Custom, and then tries
                  # to call out to all kinds of silly tools that GHC doesn't really provide.
                  # For this reason, we try to get away without re-installing lib:ghc for now.
                  reinstallableLibGhc = false;
                })

              ({ pkgs, ... }:
                # Database tests
                let
                  postgresTest = {
                    build-tools = [ pkgs.pkgsBuildHost.postgresql_12 ];
                    inherit preCheck;
                    inherit postCheck;
                  };
                in {
                  packages.cardano-db.components.tests.test-db = postgresTest;
                  packages.cardano-chain-gen.components.tests.cardano-chain-gen =
                    postgresTest;
                })

              ({
                packages.double-conversion.ghcOptions = [
                  # stop putting U __gxx_personality_v0 into the library!
                  "-optcxx-fno-rtti"
                  "-optcxx-fno-exceptions"
                  # stop putting U __cxa_guard_release into the library!
                  "-optcxx-std=gnu++98"
                  "-optcxx-fno-threadsafe-statics"
                ];
              })

              (lib.mkIf pkgs.haskell-nix.haskellLib.isCrossHost {
                packages.text-icu.patches = [
                  # Fix the following compilation error when cross-compiling:
                  #
                  # Char.hsc: In function ‘_hsc2hs_test45’:
                  # Char.hsc:1227:20: error: storage size of ‘test_array’ isn’t constant
                  # Char.hsc:1227:20: warning: unused variable ‘test_array’ [8;;https://gcc.gnu.org/onlinedocs/gcc/Warning-Options.html#index-Wunused-variable-Wunused-variable8;;]
                  # compilation failed
                  #
                  # U_NO_NUMERIC_VALUE is defined to be `((double)-123456789.)` which gcc can't
                  # figure out at compile time.
                  #
                  # More context:
                  #
                  # https://github.com/haskell/text-icu/issues/7
                  # https://gitlab.haskell.org/ghc/ghc/-/issues/7983
                  # https://gitlab.haskell.org/ghc/ghc/-/issues/12849
                  (builtins.toFile "text-icu.patch" ''
                    diff --git c/Data/Text/ICU/Char.hsc i/Data/Text/ICU/Char.hsc
                    index 6ea2b39..a0bf995 100644
                    --- c/Data/Text/ICU/Char.hsc
                    +++ i/Data/Text/ICU/Char.hsc
                    @@ -1223,7 +1223,7 @@ digitToInt c
                     -- fractions, negative, or too large to fit in a fixed-width integral type.
                     numericValue :: Char -> Maybe Double
                     numericValue c
                    -    | v == (#const U_NO_NUMERIC_VALUE) = Nothing
                    +    | v == -123456789 = Nothing
                         | otherwise                        = Just v
                         where v = u_getNumericValue . fromIntegral . ord $ c
                  '')
                ];
              })
            ];
          })).appendOverlays [
            # Collect local package `exe`s
            nixpkgs.haskell-nix.haskellLib.projectOverlays.projectComponents

            # Add git revision to `exe`s
            (final: prev: {
              hsPkgs = final.pkgs.setGitRevForPaths (self.rev or "dirty") [
                "cardano-db-sync.components.exes.cardano-db-sync"
                "cardano-smash-server.components.exes.cardano-smash-server"
                "cardano-db-tool.components.exes.cardano-db-tool"
              ] prev.hsPkgs;
            })

            (final: prev: {
              profiled = final.appendModule {
                modules = [{
                  enableLibraryProfiling = true;
                  enableProfiling = true;
                  packages.cardano-db-sync.configureFlags =
                    ["--ghc-option=-fprof-auto"];
                  packages.cardano-smash-server.configureFlags =
                    ["--ghc-option=-fprof-auto"];
                }];
              };
            })
          ];

          staticChecks =
            let
              inherit (project.args) src compiler-nix-name;
            in
              with nixpkgs; {
                hlint = callPackage hlintCheck { inherit src hlint; };
                fourmolu = callPackage fourmoluCheck { inherit src fourmolu; };
                shellcheck = callPackage shellCheck { inherit src; };
              };

          flake = project.flake {};
        in with nixpkgs; lib.recursiveUpdate flake (
          let
            mkDist = platform: project:
              let
                exes = lib.collect lib.isDerivation project.exes;
                name = "cardano-db-sync-${version}-${platform}";
                version = project.exes.cardano-db-sync.identifier.version;
                env = {
                  nativeBuildInputs = with nixpkgs; [ haskellBuildUtils bintools ];
                };
              in
                nixpkgs.runCommand name env
                  ''
                    mkdir -p $out release
                    cd release

                    # Copy exes to intermediate dir
                    cp \
                      --no-clobber \
                      --remove-destination \
                      --verbose \
                      ${lib.concatMapStringsSep " " (exe: "${exe}/bin/*") exes} \
                       ./

                    # Rewrite libs on macos (from iohk-utils)
                    ${lib.optionalString
                       (platform == "macos")
                       (lib.concatMapStrings
                         (exe: "rewrite-libs . ${exe}/bin/*")
                         exes)}

                    # Package distribution
                    dist_file=${name}.tar.gz
                    tar -czf $out/$dist_file .

                    # Write summary file
                    mkdir $out/nix-support
                    echo "file binary-dist $out/$dist_file" > $out/nix-support/hydra-build-products
                  '';

            cardano-db-sync-linux = mkDist "linux" project.projectCross.musl64;
            cardano-db-sync-macos = mkDist "macos" project;
            cardano-smash-server-no-basic-auth = (project.appendModule {
              modules = [{packages.cardano-smash-server.flags.disable-basic-auth = true;}];
            }).exes.cardano-smash-server;

            docker = callPackage ./nix/docker.nix {
              inherit (inputs.iohkNix.lib) evalService;
            };
            inherit (docker) cardano-db-sync-docker cardano-smash-server-docker;

            profiled = {
              inherit (project.profiled.exes) cardano-db-sync cardano-smash-server;
            };

            # TODO: macOS builders are resource-constrained and cannot run the detabase
            # integration tests. Add these back when we get beefier builders.
            nonRequiredMacOSPaths = [
              "checks.cardano-chain-gen:test:cardano-chain-gen"
              "checks.cardano-db:test:test-db"
              "ghc96.checks.cardano-chain-gen:test:cardano-chain-gen"
              "ghc96.checks.cardano-db:test:test-db"
              "ghc98.checks.cardano-chain-gen:test:cardano-chain-gen"
              "ghc98.checks.cardano-db:test:test-db"
            ];

            nonRequiredPaths =
              if hostPlatform.isMacOS then
                nonRequiredMacOSPaths
              else [];

          in rec {
            checks = staticChecks;

            hydraJobs = callPackages inputs.iohkNix.utils.ciJobsAggregates {
              ciJobs = flake.hydraJobs;
              nonRequiredPaths = map lib.hasPrefix nonRequiredPaths;
            } // lib.optionalAttrs (system == "x86_64-linux") {
              inherit
                cardano-db-sync-linux
                cardano-db-sync-docker
                cardano-smash-server-docker;

              # No need to run static checks on all architectures
              checks = staticChecks;
            } // lib.optionalAttrs (system == "x86_64-darwin") {
              inherit cardano-db-sync-macos;
            } // {
              inherit cardano-smash-server-no-basic-auth profiled;
            };

            legacyPackages = pkgs;

            packages = lib.optionalAttrs (system == "x86_64-linux") {
              inherit
                cardano-db-sync-linux
                cardano-db-sync-docker
                cardano-smash-server-docker
                project;

              default = flake.packages."cardano-db-sync:exe:cardano-db-sync";
            } // lib.optionalAttrs (system == "x86_64-darwin") {
              inherit cardano-db-sync-macos;
            } // {
              inherit cardano-smash-server-no-basic-auth profiled;
            };
          })) // {
            nixosModules = {
              cardano-db-sync = { pkgs, lib, ... }: {
                imports = [ ./nix/nixos/cardano-db-sync-service.nix ];
                services.cardano-db-sync.dbSyncPkgs =
                  let
                    pkgs' = self.legacyPackages.${pkgs.system};
                  in {
                    inherit (pkgs')
                      cardanoLib
                      schema
                      cardano-db-sync
                      cardano-db-sync-profiled;

                    # cardano-db-tool
                    cardanoDbSyncHaskellPackages.cardano-db-tool.components.exes.cardano-db-tool =
                      pkgs'.cardano-db-tool;
                };
              };
            };
          };

  nixConfig = {
    extra-substituters = [ "https://cache.iog.io" ];
    extra-trusted-public-keys = [ "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ=" ];
    allow-import-from-derivation = true;
    experimental-features = [ "nix-command" "flakes" "fetch-closure" ];
  };
}
