import csv
from collections import defaultdict

def load_lookup_table(file_path):
    lookup_table = {}
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = (row['dstport'], row['protocol'].lower())
            lookup_table[key] = row['tag']
    return lookup_table

def process_flow_logs(log_file, lookup_table):
    tag_counts = defaultdict(int)
    port_protocol_counts = defaultdict(int)
    
    with open(log_file, mode='r') as file:
        for line in file:
            parts = line.split()
            if len(parts) < 13:
                continue
            
            dstport = parts[5]
            protocol = parts[7]  # Typically the protocol is the 7th column, but you should verify with your data
            
            # Convert protocol number to text (6 -> tcp, 17 -> udp)
            protocol_name = protocol_name_from_number(protocol)
            
            key = (dstport, protocol_name)
            tag = lookup_table.get(key, "Untagged")
            
            tag_counts[tag] += 1
            port_protocol_counts[key] += 1
    
    return tag_counts, port_protocol_counts

def protocol_name_from_number(protocol_number):
    protocol_map = {
        '6': 'tcp',
        '17': 'udp',
        # Add more mappings if needed
    }
    return protocol_map.get(protocol_number, 'unknown')

def write_output(tag_counts, port_protocol_counts, output_file):
    with open(output_file, mode='w') as file:
        file.write("Tag Counts:\n")
        file.write("Tag,Count\n")
        for tag, count in tag_counts.items():
            file.write(f"{tag},{count}\n")
        
        file.write("\nPort/Protocol Combination Counts:\n")
        file.write("Port,Protocol,Count\n")
        for (port, protocol), count in port_protocol_counts.items():
            file.write(f"{port},{protocol},{count}\n")

def main():
    lookup_file = 'Logs/lookup_table.csv'
    log_file = 'Logs/flow_logs.txt'
    output_file = 'output_results.csv'

    lookup_table = load_lookup_table(lookup_file)
    tag_counts, port_protocol_counts = process_flow_logs(log_file, lookup_table)
    write_output(tag_counts, port_protocol_counts, output_file)

if __name__ == "__main__":
    main()
