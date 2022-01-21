import kachery_client as kc

def main():
    kec = kc.EphemeralClient(channel='flatiron1')
    # a = kec.load_file('sha1://70bc5abdfd5455e16529ffc2148310488f797d0a/jinjaroot.yaml')
    a = kec.load_file('sha1://7980d3da022a8e9e50eb245b7a53f3fb29150488/Freenove_Ultimate_Starter_Kit_for_ESP32-master.zip?manifest=753f8798f7948dec8d2ed4871c886a47fd45990a')
    print(a)

if __name__ == '__main__':
    main()