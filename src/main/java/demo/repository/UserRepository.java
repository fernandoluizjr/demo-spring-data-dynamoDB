package demo.repository;

import java.util.List;

import org.socialsignin.spring.data.dynamodb.repository.EnableScan;
import org.springframework.data.repository.CrudRepository;

import demo.domain.User;

@EnableScan
public interface UserRepository extends CrudRepository<User, String> {
  List<User> findByLastName(String lastName);
  List<User> findByFirstName(String firstName);
}
