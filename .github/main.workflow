workflow "Build and test on push" {
  on = "push"
  resolves = ["Nitox Test Suite"]
}

action "Build Nitox" {
  uses = "actions/docker/cli@76ff57a"
  args = "build -t yellowinnovation/nitox .github/docker"
}

action "NATS" {
  uses = "actions/docker/cli@76ff57a"
  args = "run nats"
  needs = ["Build Nitox"]
}

action "Nitox Test Suite" {
  uses = "actions/docker/cli@76ff57a"
  needs = ["NATS"]
  args = "run yellowinnovation/nitox \"cargo test --release\""
}
