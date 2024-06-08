from collections import defaultdict
import uuid
import logging

class ConnectedComponentsFinder:
    def __init__(self, previous_uuid_map=None):
        self.previous_uuid_map = previous_uuid_map

    def find_root(self, record, record_pool):
        if record_pool[record] == record:
            return record
        root = self.find_root(record_pool[record], record_pool)
        record_pool[record] = root  # Path compression
        return root

    def union(self, a, b, record_pool):
        root_a = self.find_root(a, record_pool)
        root_b = self.find_root(b, record_pool)
        record_pool[root_b] = root_a

    def find_connected_components(self, data):
        connections = defaultdict(set)
        record_pool = {}

        for record_id, record in data.items():
            email, phone = record.get("email"), record.get("phone")
            if email:
                connections[email].add(record_id)
            if phone:
                connections[phone].add(record_id)
            record_pool[record_id] = record_id

        for connected_ids in connections.values():
            root = self.find_root(connected_ids.pop(), record_pool)
            for record_id in connected_ids:
                self.union(root, record_id, record_pool)

        components = defaultdict(list)
        for record_id in record_pool.keys():
            root = self.find_root(record_id, record_pool)
            components[root].append(record_id)

        uuid_map = {}
        if self.previous_uuid_map is not None:
            element_to_key_mapping = self.build_element_to_key_mapping(self.previous_uuid_map)
            logging.info(f"Start matching keys for components for {len(components)} records")

            for component in components.values():
                existing_uuid = self.find_matching_key(component, element_to_key_mapping)
                if existing_uuid is not None:
                    uuid_map[existing_uuid] = list(set(component))
                else:
                    uuid_map[str(uuid.uuid4())] = component
        else:
            for component in components.values():
                uuid_map[str(uuid.uuid4())] = component

        result = [
            {
                "email": data[record_id].get("email"),
                "phone": data[record_id].get("phone"),
                "record_id": record_id,
                "uuid": uuid
            }
            for uuid, component in uuid_map.items()
            for record_id in component
        ]

        return result, uuid_map

    def build_element_to_key_mapping(self, uuid_map):
        element_to_key_mapping = {}
        for key, values in uuid_map.items():
            for value in values:
                element_to_key_mapping[value] = key
        return element_to_key_mapping

    def find_matching_key(self, records_list, element_to_key_mapping):
        matching_element = next((element for element in records_list if element in element_to_key_mapping), None)
        if matching_element is not None:
            matching_key = element_to_key_mapping[matching_element]
            return matching_key
        return None