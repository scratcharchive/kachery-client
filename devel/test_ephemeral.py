import kachery_client as kc

def main():
    kc.enable_ephemeral()
    uri = 'sha1://5865d31c4136766c6056edf6a5bf3a3a547b8427/ephemeral_hello.txt'
    txt = kc.load_text(uri)
    assert txt is not None
    print(txt)
    print(kc.load_file(uri))

    uri = kc.store_text('test ephemeral 1')
    print(uri)
    print(kc.load_file(uri))

if __name__ == '__main__':
    main()