from nanoflow import Flow, task


@task
def add(a, b):
    return a + b


def test_task_call_outside_flow_behaves_like_plain_function():
    assert add(2, 3) == 5


def test_task_call_inside_flow_registers_node_without_executing():
    calls = {"n": 0}

    @task
    def counted():
        calls["n"] += 1
        return 1

    with Flow("t", sinks=[]) as f:
        counted()
    assert calls["n"] == 0
    f.run()
    assert calls["n"] == 1


def test_task_run_direct_bypasses_flow_and_does_not_register_a_node():
    with Flow("t", sinks=[]) as f:
        result = add.run_direct(2, 3)
    assert result == 5
    assert f.graph.nodes == {}


def test_task_repr():
    assert repr(add) == "Task('add')"


def test_task_name_defaults_to_function_name_and_can_be_overridden():
    @task
    def my_fn():
        return None

    @task(name="custom")
    def other_fn():
        return None

    assert my_fn.name == "my_fn"
    assert other_fn.name == "custom"
