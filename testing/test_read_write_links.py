from tide_tooling import ReadWriteLink, StackLink, TIDE


def test_read_write_link_accepts_structured_fields_and_preserves_display():
    link = ReadWriteLink(
        type="read",
        ref="global:@A",
        ver=0,
        producer="Producer",
        action="read",
        relation="from",
    )

    assert link.type == "read"
    assert link.ref == "global:@A"
    assert link.ver == 0
    assert link.producer == "Producer"
    assert link.consumer is None
    assert str(link) == "read global:@A[v0] from Producer"
    assert link.to_dict()["display"] == str(link)


def test_read_write_link_accepts_legacy_terminal_text():
    read = ReadWriteLink("read as global:@A[v0] from Producer")
    write = ReadWriteLink.from_text("written as global:@B[v1]")

    assert (read.type, read.ref, read.ver) == ("read", "global:@A", 0)
    assert read.producer == "Producer"
    assert read.relation == "from"
    assert str(read) == "read as global:@A[v0] from Producer"

    assert (write.type, write.ref, write.ver) == ("write", "global:@B", 1)
    assert write.producer is None
    assert str(write) == "written as global:@B[v1]"


def test_trace_paths_contain_typed_terminal_hops_and_keep_show_output():
    tide = TIDE()
    tide.event("Producer", writes={"@A": "a"})
    tide.event("Consumer", reads=["@A"], writes={"@B": "b"})
    trace = tide.trace()

    source = trace.inspect("global:@A", "reference", version=0)
    target = trace.inspect("Consumer", "actor", grouped=False)
    path = trace.path(source, target)
    hops = path.get_path()

    assert isinstance(hops[0], StackLink)
    assert isinstance(hops[1], ReadWriteLink)
    assert hops[1].type == "read"
    assert hops[1].ref == "global:@A"
    assert hops[1].ver == 0
    assert hops[1].producer == trace.exec_log[0].producer_id
    assert str(hops[1]) == "read global:@A[v0] from Producer"

    output = path.show(color=False)
    assert "read global:@A[v0] from Producer" in output

    path_data = next(iter(path.to_dict()["path_by_root"].values()))
    terminal = path_data[0][1]
    assert terminal["type"] == "read"
    assert terminal["ref"] == "global:@A"
    assert terminal["version"] == 0
