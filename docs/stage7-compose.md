# Phase 7 Compose Runbook

Run `scripts/phase7/up.ps1 -Mode full` for the default topology. The first run
copies `compose/.env.example` to `compose/.env` and stops until values are
reviewed. `light` starts only Redis-backed tenant traffic; `obs` and `ha` add
local acceptance services. Every command uses project name `trpc-phase7`.

Use `sql-init -kind all` once and `sql-ready -kind all` on subsequent starts.
Never use `down -v` while testing persistence. `down.ps1` leaves named volumes
intact unless `-Volumes` is explicitly supplied.

For a minimal real WeCom + DeepSeek run, set `IDENTITY_SECRET` and either the
`PHASE7_*` credentials or the existing compatible `PHASE5_*` credentials, then
run `scripts/phase7/real-wecom.ps1 -Action Start`. The dedicated Compose project
contains only Redis, Gateway and Worker. Use `-Action Status` to inspect it and
`-Action Stop` to stop it without deleting its volume. Credential values remain
in process/container environment and are never written to a generated file.
