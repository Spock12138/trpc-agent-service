# Phase 7 Compose Runbook

Run `scripts/phase7/up.ps1 -Mode full` for the default topology. The first run
copies `compose/.env.example` to `compose/.env` and stops until values are
reviewed. `light` starts only Redis-backed tenant traffic; `obs` and `ha` add
local acceptance services. Every command uses project name `trpc-phase7`.

Use `sql-init -kind all` once and `sql-ready -kind all` on subsequent starts.
Never use `down -v` while testing persistence. `down.ps1` leaves named volumes
intact unless `-Volumes` is explicitly supplied.
