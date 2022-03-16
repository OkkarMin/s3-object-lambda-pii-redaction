import { faker } from "@faker-js/faker";
import fs from "fs";

const data: Array<object> = [];

for (let index = 0; index < 100; index++) {
  data.push({
    name: faker.name.findName(),
    age: faker.datatype.number({ min: 18, max: 70 }).toString(),
    address: faker.address.streetAddress(),
    department: faker.commerce.department(),
    email: faker.internet.email(),
    username: faker.internet.userName(),
    phone: faker.phone.phoneNumber(),
  });
}

fs.writeFile("output.json", JSON.stringify(data), "utf-8", function (err) {
  if (err) console.log(err);
});
