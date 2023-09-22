{
  description = "Java client metrics test";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/master";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          devShell = pkgs.mkShell {
            hardeningDisable = [ "all" ];
            buildInputs = with pkgs;[ jdk8 maven gcc ];
            shellHook = ''
              export JAVA_HOME=${pkgs.jdk8}
              PATH="${pkgs.jdk8}/bin:$PATH"
              ln -sf "${pkgs.jdk8}/lib/openjdk" .jdk
            '';
          };
        }
      );
}
